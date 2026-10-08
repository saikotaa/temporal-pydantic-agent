"""Command contract accepted by agent-api and forwarded to the conversation workflow.

Commands are the only way to change a conversation. The payload is a union discriminated on
`kind`; `send` is the only kind that may create the workflow (update-with-start), everything
else is delivered as a signal to an existing workflow.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, Field

from toy.state import ConversationStatus


class CommandKind(StrEnum):
    SEND = "send"
    STEER = "steer"
    CONTEXT = "context"
    EDIT = "edit"
    DELETE = "delete"
    REORDER = "reorder"
    SEND_NOW = "send_now"
    STOP = "stop"
    TOOL_RESULTS = "tool_results"


class ApprovalDecision(StrEnum):
    ACCEPT = "accept"
    DECLINE = "decline"


class ToolResult(BaseModel):
    tool_call_id: str
    decision: ApprovalDecision
    reason: str | None = None


class SendPayload(BaseModel):
    kind: Literal[CommandKind.SEND] = CommandKind.SEND
    content: str = Field(min_length=1)


class SteerPayload(BaseModel):
    """Extra instruction for the running turn (or queued if nothing runs)."""

    kind: Literal[CommandKind.STEER] = CommandKind.STEER
    content: str = Field(min_length=1)


class ContextPayload(BaseModel):
    """Extra context for the running turn (or queued if nothing runs)."""

    kind: Literal[CommandKind.CONTEXT] = CommandKind.CONTEXT
    content: str = Field(min_length=1)


class EditPayload(BaseModel):
    kind: Literal[CommandKind.EDIT] = CommandKind.EDIT
    target_client_message_id: str
    content: str = Field(min_length=1)


class DeletePayload(BaseModel):
    kind: Literal[CommandKind.DELETE] = CommandKind.DELETE
    target_client_message_id: str


class ReorderPayload(BaseModel):
    """New order of `pending`, as client message ids. Ids not listed keep their relative
    order after the listed ones; unknown ids are ignored."""

    kind: Literal[CommandKind.REORDER] = CommandKind.REORDER
    order: list[str]


class SendNowPayload(BaseModel):
    kind: Literal[CommandKind.SEND_NOW] = CommandKind.SEND_NOW
    target_client_message_id: str


class StopPayload(BaseModel):
    """Cancel the running turn. With `expected_turn_id` the stop is ignored if a different
    turn is running (scoped cancel)."""

    kind: Literal[CommandKind.STOP] = CommandKind.STOP
    expected_turn_id: str | None = None


class ToolResultsPayload(BaseModel):
    kind: Literal[CommandKind.TOOL_RESULTS] = CommandKind.TOOL_RESULTS
    results: list[ToolResult] = Field(min_length=1)


CommandPayload = Annotated[
    SendPayload
    | SteerPayload
    | ContextPayload
    | EditPayload
    | DeletePayload
    | ReorderPayload
    | SendNowPayload
    | StopPayload
    | ToolResultsPayload,
    Field(discriminator="kind"),
]


class Command(BaseModel):
    conversation_id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    client_message_id: str = Field(min_length=1, max_length=128)
    payload: CommandPayload


class CommandAck(BaseModel):
    accepted: bool
    status: ConversationStatus
    duplicate: bool = False
    detail: str | None = None
    workflow_id: str | None = None
