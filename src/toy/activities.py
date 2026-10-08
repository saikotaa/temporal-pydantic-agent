"""Activities with I/O: Postgres persistence, status mirror, event publishing.

Model and tool activities are registered by Pydantic AI's `TemporalDurability`; these are ours.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel
from pydantic_ai.messages import ModelMessage
from temporalio import activity

from toy.db import ConversationRepository, StatusRecord, TurnRecord
from toy.events import StreamEvent
from toy.runtime import EventPublisher
from toy.state import ConversationStatus, TurnTerminal


class PersistTurnInput(BaseModel):
    conversation_id: str
    turn_id: str
    messages: list[ModelMessage]
    terminal: TurnTerminal
    usage: dict[str, Any] | None = None


class MirrorStatusInput(BaseModel):
    conversation_id: str
    status: ConversationStatus
    pending_count: int
    turn_count: int


class ConversationActivities:
    def __init__(self, repository: ConversationRepository, publisher: EventPublisher) -> None:
        self._repo = repository
        self._publisher = publisher

    @activity.defn
    async def load_history(self, conversation_id: str) -> list[ModelMessage]:
        return await self._repo.load_history(conversation_id)

    @activity.defn
    async def persist_turn(self, data: PersistTurnInput) -> None:
        await self._repo.persist_turn(
            TurnRecord(
                conversation_id=data.conversation_id,
                turn_id=data.turn_id,
                messages=data.messages,
                terminal=data.terminal,
                usage=data.usage,
            )
        )

    @activity.defn
    async def mirror_status(self, data: MirrorStatusInput) -> None:
        await self._repo.mirror_status(
            StatusRecord(
                conversation_id=data.conversation_id,
                status=data.status,
                pending_count=data.pending_count,
                turn_count=data.turn_count,
            )
        )

    @activity.defn
    async def publish_event(self, event: StreamEvent) -> str:
        return await self._publisher.publish(event)

    @property
    def all(self) -> list[Any]:
        return [self.load_history, self.persist_turn, self.mirror_status, self.publish_event]
