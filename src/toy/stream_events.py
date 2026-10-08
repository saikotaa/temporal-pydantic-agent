"""Pydantic AI stream events -> SDK-independent `StreamEvent`s -> Redis.

`publish_events` is the agent's `event_stream_handler`. With `TemporalDurability` it runs inside
the model-request activity for model events (so tokens leave the activity as they arrive) and in
a small per-event activity for tool events. `normalize` is pure and unit-testable.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterable
from dataclasses import dataclass
from typing import Any

from pydantic_ai import RunContext
from pydantic_ai.messages import (
    AgentStreamEvent,
    FunctionToolResultEvent,
    PartDeltaEvent,
    PartEndEvent,
    PartStartEvent,
    RetryPromptPart,
    TextPart,
    TextPartDelta,
    ThinkingPart,
    ThinkingPartDelta,
    ToolCallPart,
    ToolCallPartDelta,
    ToolReturnPart,
)
from temporalio import activity

from toy.events import EventType, StreamEvent
from toy.runtime import runtime


@dataclass
class ConversationDeps:
    """Serializable deps handed to every activity of a turn."""

    conversation_id: str
    turn_id: str


def _attempt() -> int:
    return activity.info().attempt if activity.in_activity() else 1


def _args_dict(part: ToolCallPart) -> dict[str, Any]:
    try:
        return part.args_as_dict()
    except Exception:
        return {}


def _json_safe(value: Any) -> Any:
    try:
        json.dumps(value)
    except TypeError:
        return str(value)
    return value


def normalize(
    event: AgentStreamEvent, *, conversation_id: str, turn_id: str, attempt: int
) -> list[StreamEvent]:
    """Map one Pydantic AI event to zero or more normalized events."""

    def ev(type_: EventType, **fields: Any) -> StreamEvent:
        return StreamEvent(
            conversation_id=conversation_id, turn_id=turn_id, attempt=attempt, type=type_, **fields
        )

    match event:
        case PartStartEvent(index=index, part=TextPart(content=content)):
            out = [ev(EventType.TEXT_START, part_id=f"p{index}")]
            if content:
                out.append(ev(EventType.TEXT_DELTA, part_id=f"p{index}", delta=content))
            return out
        case PartDeltaEvent(index=index, delta=TextPartDelta(content_delta=delta)):
            return [ev(EventType.TEXT_DELTA, part_id=f"p{index}", delta=delta)] if delta else []
        case PartEndEvent(index=index, part=TextPart()):
            return [ev(EventType.TEXT_END, part_id=f"p{index}")]
        case PartStartEvent(index=index, part=ThinkingPart(content=content)):
            out = [ev(EventType.REASONING_START, part_id=f"p{index}")]
            if content:
                out.append(ev(EventType.REASONING_DELTA, part_id=f"p{index}", delta=content))
            return out
        case PartDeltaEvent(index=index, delta=ThinkingPartDelta(content_delta=delta)):
            return (
                [ev(EventType.REASONING_DELTA, part_id=f"p{index}", delta=delta)] if delta else []
            )
        case PartEndEvent(index=index, part=ThinkingPart()):
            return [ev(EventType.REASONING_END, part_id=f"p{index}")]
        case PartStartEvent(part=ToolCallPart() as part):
            out = [
                ev(
                    EventType.TOOL_INPUT_START,
                    tool_name=part.tool_name,
                    tool_call_id=part.tool_call_id,
                )
            ]
            if part.args:
                out.append(
                    ev(
                        EventType.TOOL_INPUT_DELTA,
                        tool_call_id=part.tool_call_id,
                        delta=part.args_as_json_str(),
                    )
                )
            return out
        case PartDeltaEvent(delta=ToolCallPartDelta(args_delta=args_delta, tool_call_id=call_id)):
            if not args_delta or call_id is None:
                return []
            delta = args_delta if isinstance(args_delta, str) else json.dumps(args_delta)
            return [ev(EventType.TOOL_INPUT_DELTA, tool_call_id=call_id, delta=delta)]
        case PartEndEvent(part=ToolCallPart() as part):
            return [
                ev(
                    EventType.TOOL_INPUT_AVAILABLE,
                    tool_name=part.tool_name,
                    tool_call_id=part.tool_call_id,
                    args=_args_dict(part),
                )
            ]
        case FunctionToolResultEvent(part=ToolReturnPart() as part):
            return [
                ev(
                    EventType.TOOL_OUTPUT,
                    tool_name=part.tool_name,
                    tool_call_id=part.tool_call_id,
                    output=_json_safe(part.content),
                )
            ]
        case FunctionToolResultEvent(part=RetryPromptPart() as part):
            return [
                ev(
                    EventType.TOOL_OUTPUT,
                    tool_name=part.tool_name,
                    tool_call_id=part.tool_call_id or "",
                    error=part.model_response(),
                )
            ]
        case _:
            return []


async def publish_events(
    ctx: RunContext[ConversationDeps], stream: AsyncIterable[AgentStreamEvent]
) -> None:
    """Agent `event_stream_handler`: normalize and XADD each event; heartbeat with the stream id."""
    publisher = runtime.require_publisher()
    attempt = _attempt()
    async for event in stream:
        for normalized in normalize(
            event,
            conversation_id=ctx.deps.conversation_id,
            turn_id=ctx.deps.turn_id,
            attempt=attempt,
        ):
            stream_id = await publisher.publish(normalized)
            if activity.in_activity():
                activity.heartbeat(stream_id)
