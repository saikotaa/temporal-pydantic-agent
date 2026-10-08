"""Usage-based billing: a `WrapperModel` that meters every model call idempotently.

`BillingModel` runs inside the model activity. After each request it computes usage and cost
and posts one meter event whose identifier is `uuid5(NS, "{workflow_id}:{run_id}:{activity_id}")`,
so a retried activity re-posts the same identifier and the billing endpoint (Stripe-like, 409 on
duplicates) counts it once. A request that ends in an exception after producing usage is a
partial completion and gets `:attempt{n}` appended so it is metered separately.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Protocol

import httpx
from pydantic import BaseModel
from pydantic_ai import RunContext
from pydantic_ai.messages import ModelMessage, ModelResponse
from pydantic_ai.models import Model, ModelRequestParameters, StreamedResponse
from pydantic_ai.models.wrapper import WrapperModel
from pydantic_ai.settings import ModelSettings
from pydantic_ai.usage import RequestUsage
from temporalio import activity

from toy.events import EventType, StreamEvent
from toy.runtime import runtime

log = logging.getLogger(__name__)

BILLING_NAMESPACE = uuid.UUID("7b6b2c62-1d2b-4b6e-9f0a-2d5d3c4e8a11")
METER_EVENT_NAME = "llm_tokens"
METER_EVENTS_PATH = "/v1/billing/meter_events"


class MeterOutcome(StrEnum):
    RECORDED = "recorded"
    DUPLICATE = "duplicate"


@dataclass(frozen=True)
class Price:
    """USD per million tokens."""

    input: float
    output: float
    cache_read: float = 0.0
    cache_write: float = 0.0


PRICES: dict[str, Price] = {
    "anthropic/claude-sonnet-5-5": Price(input=3.0, output=15.0, cache_read=0.3, cache_write=3.75),
    "anthropic/claude-haiku-5-5": Price(input=1.0, output=5.0, cache_read=0.1, cache_write=1.25),
    "test": Price(input=0.0, output=0.0),
}
DEFAULT_PRICE = Price(input=1.0, output=4.0)


def price_for(model_name: str) -> Price:
    return PRICES.get(model_name, DEFAULT_PRICE)


class UsageDict(BaseModel):
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0

    @classmethod
    def from_usage(cls, usage: RequestUsage) -> UsageDict:
        return cls(
            input_tokens=usage.input_tokens,
            output_tokens=usage.output_tokens,
            cache_read_tokens=usage.cache_read_tokens,
            cache_write_tokens=usage.cache_write_tokens,
        )

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens

    def cost_usd(self, price: Price) -> float:
        return round(
            (
                self.input_tokens * price.input
                + self.output_tokens * price.output
                + self.cache_read_tokens * price.cache_read
                + self.cache_write_tokens * price.cache_write
            )
            / 1_000_000,
            8,
        )


class MeterEvent(BaseModel):
    """What we send to the billing endpoint (Stripe meter event shape, JSON)."""

    event_name: str = METER_EVENT_NAME
    identifier: str
    customer: str
    value: int
    model: str
    usage: UsageDict
    cost_usd: float
    partial: bool = False
    attempt: int = 1
    conversation_id: str | None = None
    turn_id: str | None = None


class BillingClient(Protocol):
    async def record(self, event: MeterEvent) -> MeterOutcome: ...


@dataclass
class InMemoryBillingClient:
    events: dict[str, MeterEvent] = field(default_factory=dict[str, MeterEvent])
    attempts: list[str] = field(default_factory=list[str])

    async def record(self, event: MeterEvent) -> MeterOutcome:
        self.attempts.append(event.identifier)
        if event.identifier in self.events:
            return MeterOutcome.DUPLICATE
        self.events[event.identifier] = event
        return MeterOutcome.RECORDED


class HttpBillingClient:
    def __init__(self, http: httpx.AsyncClient, base_url: str) -> None:
        self._http = http
        self._base_url = base_url.rstrip("/")

    async def record(self, event: MeterEvent) -> MeterOutcome:
        response = await self._http.post(
            f"{self._base_url}{METER_EVENTS_PATH}", json=event.model_dump(mode="json")
        )
        if response.status_code == 409:
            return MeterOutcome.DUPLICATE
        response.raise_for_status()
        return MeterOutcome.RECORDED


@dataclass(frozen=True)
class CallIdentity:
    workflow_id: str
    activity_id: str
    attempt: int
    run_id: str = ""
    """Activity ids restart on continue-as-new, so the run id is part of the identity."""


def activity_identity() -> CallIdentity:
    """Identity of the running model activity; random outside Temporal (never deduped)."""
    if activity.in_activity():
        info = activity.info()
        return CallIdentity(
            info.workflow_id or "",
            info.activity_id,
            info.attempt,
            run_id=info.workflow_run_id or "",
        )
    return CallIdentity("local", uuid.uuid4().hex, 1)


def meter_identifier(identity: CallIdentity, *, partial: bool) -> str:
    base = f"{identity.workflow_id}:{identity.run_id}:{identity.activity_id}"
    if partial:
        base += f":attempt{identity.attempt}"
    return str(uuid.uuid5(BILLING_NAMESPACE, base))


class BillingModel(WrapperModel):
    """Meters each model call (streamed or not) through a `BillingClient`."""

    def __init__(
        self,
        wrapped: Model,
        client: BillingClient,
        *,
        customer: str,
        identity: Callable[[], CallIdentity] = activity_identity,
        price_model_name: str | None = None,
    ) -> None:
        super().__init__(wrapped)
        self._client = client
        self._customer = customer
        self._identity = identity
        self._price_model_name = price_model_name

    async def request(
        self,
        messages: list[ModelMessage],
        model_settings: ModelSettings | None,
        model_request_parameters: ModelRequestParameters,
    ) -> ModelResponse:
        response = await self.wrapped.request(messages, model_settings, model_request_parameters)
        await self._meter(response.usage, partial=False, run_context=None)
        return response

    @asynccontextmanager
    async def request_stream(
        self,
        messages: list[ModelMessage],
        model_settings: ModelSettings | None,
        model_request_parameters: ModelRequestParameters,
        run_context: RunContext[Any] | None = None,
    ) -> AsyncGenerator[StreamedResponse]:
        stream: StreamedResponse | None = None
        try:
            async with self.wrapped.request_stream(
                messages, model_settings, model_request_parameters, run_context
            ) as stream:
                yield stream
        except BaseException:
            if stream is not None:
                await self._meter(stream.usage, partial=True, run_context=run_context)
            raise
        else:
            await self._meter(stream.usage, partial=False, run_context=run_context)

    async def _meter(
        self, usage: RequestUsage, *, partial: bool, run_context: RunContext[Any] | None
    ) -> None:
        usage_dict = UsageDict.from_usage(usage)
        if usage_dict.total_tokens == 0:
            return
        identity = self._identity()
        model_name = self._price_model_name or self.wrapped.model_name
        deps = run_context.deps if run_context is not None else None
        event = MeterEvent(
            identifier=meter_identifier(identity, partial=partial),
            customer=self._customer,
            value=usage_dict.total_tokens,
            model=model_name,
            usage=usage_dict,
            cost_usd=usage_dict.cost_usd(price_for(model_name)),
            partial=partial,
            attempt=identity.attempt,
            conversation_id=getattr(deps, "conversation_id", None),
            turn_id=getattr(deps, "turn_id", None),
        )
        outcome = await self._client.record(event)
        log.info(
            "meter %s %s tokens=%s cost=%s", outcome, event.identifier, event.value, event.cost_usd
        )
        if (
            outcome == MeterOutcome.RECORDED
            and event.conversation_id
            and event.turn_id
            and runtime.publisher is not None
        ):
            await runtime.publisher.publish(
                StreamEvent(
                    conversation_id=event.conversation_id,
                    turn_id=event.turn_id,
                    attempt=identity.attempt,
                    type=EventType.USAGE_UPDATED,
                    usage={
                        **usage_dict.model_dump(),
                        "model": model_name,
                        "cost_usd": event.cost_usd,
                    },
                )
            )
