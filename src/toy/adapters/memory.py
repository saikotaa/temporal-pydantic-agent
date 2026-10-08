"""In-memory ports: the queue semantics without Temporal, and an event source with replay."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

from toy.commands import (
    Command,
    CommandAck,
    CommandKind,
    DeletePayload,
    EditPayload,
    ReorderPayload,
    SendNowPayload,
    ToolResultsPayload,
)
from toy.events import StreamEvent
from toy.state import (
    ConversationState,
    ConversationStatus,
    QueuedKind,
    QueuedMessage,
    QueuedToolResult,
)


def reorder_pending(pending: list[QueuedMessage], order: list[str]) -> list[QueuedMessage]:
    """Pure helper shared with the workflow: listed ids first in the given order, then the rest."""
    by_id = {m.client_message_id: m for m in pending}
    head = [by_id[i] for i in order if i in by_id]
    seen = {m.client_message_id for m in head}
    return head + [m for m in pending if m.client_message_id not in seen]


def apply_queue_command(state: ConversationState, command: Command) -> CommandAck:
    """Queue-only semantics (no turn execution). Used by the in-memory stub and as the
    reference for the workflow's handlers."""
    payload = command.payload
    seen = {m.client_message_id for m in state.pending}
    if command.client_message_id in seen:
        return CommandAck(accepted=True, status=state.status, duplicate=True)

    match payload.kind:
        case CommandKind.SEND | CommandKind.STEER | CommandKind.CONTEXT:
            kind = QueuedKind(payload.kind.value)
            content = getattr(payload, "content", "")
            state.pending.append(
                QueuedMessage(
                    client_message_id=command.client_message_id, kind=kind, content=content
                )
            )
            return CommandAck(accepted=True, status=state.status)
        case CommandKind.EDIT:
            assert isinstance(payload, EditPayload)
            for m in state.pending:
                if m.client_message_id == payload.target_client_message_id:
                    m.content = payload.content
                    return CommandAck(accepted=True, status=state.status)
            return CommandAck(accepted=False, status=state.status, detail="not in queue")
        case CommandKind.DELETE:
            assert isinstance(payload, DeletePayload)
            before = len(state.pending)
            state.pending = [
                m for m in state.pending if m.client_message_id != payload.target_client_message_id
            ]
            ok = len(state.pending) != before
            return CommandAck(
                accepted=ok, status=state.status, detail=None if ok else "not in queue"
            )
        case CommandKind.REORDER:
            assert isinstance(payload, ReorderPayload)
            state.pending = reorder_pending(state.pending, payload.order)
            return CommandAck(accepted=True, status=state.status)
        case CommandKind.SEND_NOW:
            assert isinstance(payload, SendNowPayload)
            if payload.target_client_message_id not in seen:
                return CommandAck(accepted=False, status=state.status, detail="not in queue")
            state.pending = reorder_pending(state.pending, [payload.target_client_message_id])
            return CommandAck(accepted=True, status=state.status)
        case CommandKind.STOP:
            if state.status != ConversationStatus.WORKING:
                return CommandAck(accepted=False, status=state.status, detail="no turn running")
            return CommandAck(accepted=True, status=state.status)
        case CommandKind.TOOL_RESULTS:
            assert isinstance(payload, ToolResultsPayload)
            if state.status != ConversationStatus.BLOCKED:
                return CommandAck(accepted=False, status=state.status, detail="not blocked")
            results = [
                QueuedToolResult(
                    tool_call_id=r.tool_call_id,
                    approved=r.decision == "accept",
                    reason=r.reason,
                )
                for r in payload.results
            ]
            state.pending.insert(
                0,
                QueuedMessage(
                    client_message_id=command.client_message_id,
                    kind=QueuedKind.TOOL_RESULTS,
                    tool_results=results,
                ),
            )
            return CommandAck(accepted=True, status=state.status)


@dataclass
class InMemoryWorkflowClient:
    """Stub with the queue semantics but no turn execution; `status` can be forced for tests."""

    states: dict[str, ConversationState] = field(default_factory=dict[str, ConversationState])

    async def submit(self, command: Command) -> CommandAck:
        state = self.states.get(command.conversation_id)
        if state is None:
            if command.payload.kind != CommandKind.SEND:
                return CommandAck(
                    accepted=False, status=ConversationStatus.IDLE, detail="conversation not found"
                )
            state = self.states[command.conversation_id] = ConversationState()
        ack = apply_queue_command(state, command)
        ack.workflow_id = f"conv:{command.conversation_id}"
        return ack

    async def get_state(self, conversation_id: str) -> ConversationState | None:
        state = self.states.get(conversation_id)
        return state.model_copy(deep=True) if state else None


@dataclass
class InMemoryEventSource:
    """Append-only per-conversation log with monotonic ids `N-0` (same shape as Redis)."""

    logs: dict[str, list[tuple[str, StreamEvent]]] = field(
        default_factory=dict[str, list[tuple[str, StreamEvent]]]
    )
    _cond: asyncio.Condition = field(default_factory=asyncio.Condition)

    async def publish(self, event: StreamEvent) -> str:
        async with self._cond:
            log = self.logs.setdefault(event.conversation_id, [])
            stream_id = f"{len(log) + 1}-0"
            log.append((stream_id, event))
            self._cond.notify_all()
            return stream_id

    async def tail(
        self, conversation_id: str, after: str | None
    ) -> AsyncIterator[tuple[str, StreamEvent]]:
        cursor = int(after.split("-")[0]) if after else 0
        while True:
            async with self._cond:
                log = self.logs.get(conversation_id, [])
                batch = log[cursor:]
                if not batch:
                    await self._cond.wait()
                    continue
                cursor = len(log)
            for item in batch:
                yield item
