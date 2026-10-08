"""SDK-independent stream events.

These are what the model activity writes to the Redis Stream `conv:{id}:events` and what the
API tails. They know nothing about Pydantic AI or the Vercel AI SDK; adapters on both sides
translate. Redis fields are flat strings so any client in any language can read them.
"""

from __future__ import annotations

import json
from enum import StrEnum
from typing import Any

from pydantic import BaseModel

from toy.state import ConversationStatus, TurnTerminal


class EventType(StrEnum):
    TEXT_START = "text.start"
    TEXT_DELTA = "text.delta"
    TEXT_END = "text.end"
    REASONING_START = "reasoning.start"
    REASONING_DELTA = "reasoning.delta"
    REASONING_END = "reasoning.end"
    TOOL_INPUT_START = "tool.input_start"
    TOOL_INPUT_DELTA = "tool.input_delta"
    TOOL_INPUT_AVAILABLE = "tool.input_available"
    TOOL_OUTPUT = "tool.output"
    TURN_STARTED = "turn.started"
    TURN_COMPLETED = "turn.completed"
    STATUS_CHANGED = "status.changed"
    USAGE_UPDATED = "usage.updated"
    REQUEST_RESOLVED = "request.resolved"


_SCALAR_FIELDS = (
    "conversation_id",
    "turn_id",
    "attempt",
    "type",
    "part_id",
    "delta",
    "tool_name",
    "tool_call_id",
    "error",
    "status",
    "terminal",
    "pending_count",
    "client_message_id",
)
_JSON_FIELDS = ("args", "output", "usage", "approvals")


class StreamEvent(BaseModel):
    conversation_id: str
    turn_id: str
    attempt: int = 1
    type: EventType
    # part fields
    part_id: str | None = None
    delta: str | None = None
    tool_name: str | None = None
    tool_call_id: str | None = None
    args: Any | None = None
    output: Any | None = None
    error: str | None = None
    # turn / status fields
    status: ConversationStatus | None = None
    terminal: TurnTerminal | None = None
    pending_count: int | None = None
    client_message_id: str | None = None
    usage: dict[str, Any] | None = None
    approvals: list[dict[str, Any]] | None = None

    def to_redis_fields(self) -> dict[str, str]:
        fields: dict[str, str] = {}
        for name in _SCALAR_FIELDS:
            value = getattr(self, name)
            if value is not None:
                fields[name] = str(value)
        for name in _JSON_FIELDS:
            value = getattr(self, name)
            if value is not None:
                fields[name] = json.dumps(value, separators=(",", ":"), default=str)
        return fields

    @classmethod
    def from_redis_fields(cls, fields: dict[str, str] | dict[bytes, bytes]) -> StreamEvent:
        decoded: dict[str, Any] = {}
        for k, v in fields.items():
            key = k.decode() if isinstance(k, bytes) else k
            val = v.decode() if isinstance(v, bytes) else v
            decoded[key] = json.loads(val) if key in _JSON_FIELDS else val
        return cls.model_validate(decoded)


def stream_key(conversation_id: str) -> str:
    return f"conv:{conversation_id}:events"


class UsageSnapshot(BaseModel):
    """Usage shape carried in `usage.updated` and `turn.completed` events."""

    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0
    requests: int = 0
    model: str | None = None
    extra: dict[str, Any] = {}
