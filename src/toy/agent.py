"""The Pydantic AI agent: instructions, tools (one needs approval), Temporal durability.

Built once at import time (Temporal requires it outside workflow code). The model and the event
publisher are resolved at call time through `toy.runtime`, so the same agent object serves the
worker (LiteLLM) and the tests (`TestModel`/`FunctionModel`).
"""

from __future__ import annotations

import hashlib
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any, cast

from pydantic_ai import Agent, DeferredToolRequests, RunContext, Tool
from pydantic_ai.capabilities import ProcessHistory
from pydantic_ai.durable_exec.temporal import TemporalDurability
from pydantic_ai.messages import ModelMessage, ModelRequest, ModelResponse, UserPromptPart
from pydantic_ai.models import Model, ModelRequestParameters, StreamedResponse
from pydantic_ai.profiles import ModelProfile
from pydantic_ai.settings import ModelSettings
from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.workflow import ActivityConfig

from toy.runtime import runtime
from toy.stream_events import ConversationDeps, publish_events

AGENT_NAME = "toy_conversation"

INSTRUCTIONS = (
    "You are a concise assistant in a toy support console. Use tools when they help. "
    "`create_ticket` needs human approval; if it is denied, say so and do not retry it."
)


class RuntimeModel(Model):
    """Delegates to whatever model `toy.runtime` currently holds (resolved inside the activity)."""

    @property
    def _target(self) -> Model:
        return runtime.require_model()

    @property
    def model_name(self) -> str:
        return runtime.model.model_name if runtime.model else "runtime"

    @property
    def system(self) -> str:
        return runtime.model.system if runtime.model else "runtime"

    @property
    def profile(self) -> ModelProfile:  # pyright: ignore[reportIncompatibleVariableOverride]
        return runtime.model.profile if runtime.model else super().profile

    async def request(
        self,
        messages: list[ModelMessage],
        model_settings: ModelSettings | None,
        model_request_parameters: ModelRequestParameters,
    ) -> ModelResponse:
        return await self._target.request(messages, model_settings, model_request_parameters)

    @asynccontextmanager
    async def request_stream(
        self,
        messages: list[ModelMessage],
        model_settings: ModelSettings | None,
        model_request_parameters: ModelRequestParameters,
        run_context: RunContext[Any] | None = None,
    ) -> AsyncGenerator[StreamedResponse]:
        async with self._target.request_stream(
            messages, model_settings, model_request_parameters, run_context
        ) as response_stream:
            yield response_stream


# --- tools -------------------------------------------------------------------------------------

_NOTES = {
    "runbook": "Restart the worker with `uv run agent-worker`; it drains gracefully for 60s.",
    "oncall": "On-call this week: sam. Escalate through the #ops channel.",
}


async def lookup_weather(city: str) -> str:
    """Look up the current weather for a city (canned data)."""
    return f"The weather in {city} is 21°C and sunny."


async def read_note(key: str) -> str:
    """Read an internal note by key. Known keys: runbook, oncall."""
    return _NOTES.get(key, f"no note named {key!r}")


async def create_ticket(title: str, body: str) -> str:
    """Create a support ticket. Requires human approval before it runs."""
    digest = hashlib.sha1(f"{title}\n{body}".encode()).hexdigest()[:4]
    return f"ticket TCK-{digest} created: {title}"


# --- steer injection (runs in workflow code, deterministic) ------------------------------------


def drain_inbox() -> list[str]:
    """Pop steer/context lines queued for the running turn from the workflow instance."""
    if not workflow.in_workflow():
        return []
    drain = cast(Callable[[], list[str]] | None, getattr(workflow.instance(), "drain_inbox", None))
    return drain() if drain is not None else []


async def inject_steering(
    ctx: RunContext[ConversationDeps], messages: list[ModelMessage]
) -> list[ModelMessage]:
    """History processor: append queued steer/context as user parts to the pending request."""
    lines = drain_inbox()
    if not lines or not messages:
        return messages
    last = messages[-1]
    if not isinstance(last, ModelRequest):
        return messages
    extra = [UserPromptPart(content=line) for line in lines]
    messages[-1] = ModelRequest(parts=[*last.parts, *extra], instructions=last.instructions)
    return messages


# --- agent -------------------------------------------------------------------------------------

durability: TemporalDurability[ConversationDeps] = TemporalDurability(
    name=AGENT_NAME,
    event_stream_handler=publish_events,
    activity_config=ActivityConfig(start_to_close_timeout=timedelta(minutes=2)),
    model_activity_config=ActivityConfig(
        start_to_close_timeout=timedelta(minutes=10),
        heartbeat_timeout=timedelta(seconds=30),
        retry_policy=RetryPolicy(maximum_attempts=3),
    ),
)

conversation_agent: Agent[ConversationDeps, str | DeferredToolRequests] = Agent(
    RuntimeModel(),
    name=AGENT_NAME,
    instructions=INSTRUCTIONS,
    deps_type=ConversationDeps,
    output_type=[str, DeferredToolRequests],
    tools=[
        Tool(lookup_weather),
        Tool(read_note),
        Tool(create_ticket, requires_approval=True),
    ],
    capabilities=[ProcessHistory(inject_steering), durability],
)
