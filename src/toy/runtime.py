"""Process-wide collaborators that must be reachable from module-level agent code.

The Pydantic AI agent (and its `TemporalDurability` capability) has to be a module-level object so
the workflow class can reference it and the worker can register its activities. The agent's model
and the event publisher are I/O objects that differ per process (worker vs tests), so the agent
is built against this holder and the entrypoint fills it in. Activities read it; workflow code
never does.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from pydantic_ai.models import Model

from toy.events import StreamEvent


class EventPublisher(Protocol):
    async def publish(self, event: StreamEvent) -> str: ...


@dataclass
class Runtime:
    model: Model | None = None
    publisher: EventPublisher | None = None

    def require_model(self) -> Model:
        if self.model is None:
            raise RuntimeError("runtime.model not configured (worker setup must set it)")
        return self.model

    def require_publisher(self) -> EventPublisher:
        if self.publisher is None:
            raise RuntimeError("runtime.publisher not configured (worker setup must set it)")
        return self.publisher


runtime = Runtime()
