"""Conversation state as exposed by the workflow query and the API."""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel


class ConversationStatus(StrEnum):
    IDLE = "idle"
    WORKING = "working"
    BLOCKED = "blocked"


class TurnTerminal(StrEnum):
    COMPLETED = "completed"
    BLOCKED = "blocked"
    STOPPED = "stopped"
    FAILED = "failed"


class QueuedKind(StrEnum):
    SEND = "send"
    STEER = "steer"
    CONTEXT = "context"
    TOOL_RESULTS = "tool_results"


class QueuedToolResult(BaseModel):
    tool_call_id: str
    approved: bool
    reason: str | None = None


class QueuedMessage(BaseModel):
    client_message_id: str
    kind: QueuedKind = QueuedKind.SEND
    content: str = ""
    tool_results: list[QueuedToolResult] = []


class PendingApproval(BaseModel):
    tool_call_id: str
    tool_name: str
    args: dict[str, Any] = {}


class ConversationState(BaseModel):
    status: ConversationStatus = ConversationStatus.IDLE
    pending: list[QueuedMessage] = []
    current_turn_id: str | None = None
    current_client_message_id: str | None = None
    turn_count: int = 0
    pending_approvals: list[PendingApproval] = []
    last_terminal: TurnTerminal | None = None
    run_number: int = 0
