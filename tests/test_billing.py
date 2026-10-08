"""BillingModel metering: idempotent identifiers, partial completions, emulator 409s."""

from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import pytest
from pydantic_ai import Agent
from pydantic_ai.messages import ModelMessage
from pydantic_ai.models.function import AgentInfo, DeltaToolCalls, FunctionModel
from pydantic_ai.models.test import TestModel

from toy.billing import (
    BillingModel,
    CallIdentity,
    HttpBillingClient,
    InMemoryBillingClient,
    MeterEvent,
    MeterOutcome,
    UsageDict,
    meter_identifier,
)
from toy.billing_emulator import create_emulator


def test_identifier_is_stable_across_attempts_and_distinct_for_partials() -> None:
    a1 = CallIdentity("conv:x", "7", 1)
    a2 = CallIdentity("conv:x", "7", 2)
    assert meter_identifier(a1, partial=False) == meter_identifier(a2, partial=False)
    assert meter_identifier(a1, partial=True) != meter_identifier(a2, partial=True)
    assert meter_identifier(a1, partial=True) != meter_identifier(a1, partial=False)
    next_run = CallIdentity("conv:x", "7", 1, run_id="run-2")
    assert meter_identifier(next_run, partial=False) != meter_identifier(a1, partial=False)


async def test_retried_call_meters_once() -> None:
    client = InMemoryBillingClient()
    model = BillingModel(
        TestModel(custom_output_text="four words of text"),
        client,
        customer="cus_t",
        identity=lambda: CallIdentity("conv:a", "act-1", 1),
    )
    agent = Agent(model)
    await agent.run("hi")
    await agent.run("hi")  # same activity identity: a retry
    assert len(client.attempts) == 2 and len(client.events) == 1
    event = next(iter(client.events.values()))
    assert event.value > 0 and event.model == "test" and event.cost_usd == 0.0 and not event.partial


async def test_partial_completion_is_metered_with_attempt() -> None:
    client = InMemoryBillingClient()

    async def stream(
        messages: list[ModelMessage], info: AgentInfo
    ) -> AsyncIterator[str | DeltaToolCalls]:
        yield "some "
        raise RuntimeError("provider dropped the connection")

    model = BillingModel(
        FunctionModel(stream_function=stream, model_name="anthropic/claude-sonnet-5-5"),
        client,
        customer="cus_t",
        identity=lambda: CallIdentity("conv:a", "act-2", 3),
    )
    agent = Agent(model)
    with pytest.raises(RuntimeError):
        async with agent.run_stream("hi") as result:
            async for _ in result.stream_text():
                pass
    assert len(client.events) == 1
    event = next(iter(client.events.values()))
    assert event.partial and event.attempt == 3
    assert event.identifier == meter_identifier(CallIdentity("conv:a", "act-2", 3), partial=True)
    assert event.cost_usd > 0


async def test_http_client_against_emulator() -> None:
    app = create_emulator()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://b"
    ) as http:
        billing = HttpBillingClient(http, "http://b")
        model = BillingModel(
            TestModel(custom_output_text="hello there"),
            billing,
            customer="cus_t",
            identity=lambda: CallIdentity("conv:a", "act-9", 1),
        )
        agent = Agent(model)
        await agent.run("hi")
        await agent.run("hi")
        listing = (await http.get("/v1/billing/meter_events", params={"customer": "cus_t"})).json()
        assert len(listing["data"]) == 1 and listing["total_value"] > 0
        assert listing["data"][0]["metadata"]["model"] == "test"
        duplicate = await billing.record(
            MeterEvent(
                identifier=listing["data"][0]["identifier"],
                customer="cus_t",
                value=1,
                model="test",
                usage=UsageDict(),
                cost_usd=0,
            )
        )
        assert duplicate == MeterOutcome.DUPLICATE
