"""Ports the API depends on. The Temporal client and Redis implement them; tests use memory."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Protocol

from toy.commands import Command, CommandAck
from toy.events import StreamEvent
from toy.state import ConversationState


class ConversationWorkflowClient(Protocol):
    async def submit(self, command: Command) -> CommandAck: ...

    async def get_state(self, conversation_id: str) -> ConversationState | None: ...


class EventSource(Protocol):
    def tail(
        self, conversation_id: str, after: str | None
    ) -> AsyncIterator[tuple[str, StreamEvent]]: ...
