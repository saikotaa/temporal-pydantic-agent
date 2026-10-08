"""`StreamEvent` -> Vercel AI SDK UI message stream chunks, framed as SSE.

One encoder instance per SSE connection (it is stateful: open parts, current turn/attempt).
Chunk models come from `pydantic_ai.ui.vercel_ai.response_types` so the wire format tracks the
SDK; everything here is the mapping from our SDK-independent events to those chunks.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from pydantic_ai.ui.vercel_ai.response_types import (
    BaseChunk,
    DataChunk,
    ErrorChunk,
    FinishChunk,
    FinishReason,
    FinishStepChunk,
    MessageMetadataChunk,
    ReasoningDeltaChunk,
    ReasoningEndChunk,
    ReasoningStartChunk,
    StartChunk,
    StartStepChunk,
    TextDeltaChunk,
    TextEndChunk,
    TextStartChunk,
    ToolInputAvailableChunk,
    ToolInputDeltaChunk,
    ToolInputStartChunk,
    ToolOutputAvailableChunk,
    ToolOutputErrorChunk,
)

from toy.events import EventType, StreamEvent
from toy.state import TurnTerminal

UI_STREAM_HEADERS = {"x-vercel-ai-ui-message-stream": "v1"}
DONE = "[DONE]"

FINISH_REASONS: dict[TurnTerminal, FinishReason] = {
    TurnTerminal.COMPLETED: "stop",
    TurnTerminal.STOPPED: "other",
    TurnTerminal.BLOCKED: "other",
    TurnTerminal.FAILED: "error",
}


class DataPart:
    """Custom `data-*` chunk types this toy emits."""

    STATUS = "data-status"
    QUEUE = "data-queue"
    APPROVAL = "data-approval"
    REQUEST_RESOLVED = "data-request-resolved"
    RETRY = "data-retry"


def sse_frame(stream_id: str, data: str) -> bytes:
    return f"id: {stream_id}\ndata: {data}\n\n".encode()


@dataclass
class VercelUiStreamEncoder:
    """Stateful encoder: call it with each (stream id, event); returns SSE bytes."""

    turn_id: str | None = None
    attempt: int = 0
    step_open: bool = False
    open_text: set[str] = field(default_factory=set[str])
    open_reasoning: set[str] = field(default_factory=set[str])

    def __call__(self, stream_id: str, event: StreamEvent) -> bytes:
        chunks = self.encode(event)
        out = b"".join(sse_frame(stream_id, c.encode(2)) for c in chunks)
        if event.type == EventType.TURN_COMPLETED:
            out += sse_frame(stream_id, DONE)
        return out

    # --- mapping -----------------------------------------------------------------------------

    def encode(self, event: StreamEvent) -> list[BaseChunk]:
        chunks: list[BaseChunk] = []
        if event.type == EventType.STATUS_CHANGED:
            data: dict[str, Any] = {"status": event.status, "turnId": event.turn_id}
            chunks.append(DataChunk(type=DataPart.STATUS, data=data, id="status"))
            chunks.append(
                DataChunk(
                    type=DataPart.QUEUE, data={"pendingCount": event.pending_count}, id="queue"
                )
            )
            return chunks
        if event.type == EventType.TURN_STARTED:
            self._begin_turn(event, chunks)
            return chunks
        if event.type == EventType.TURN_COMPLETED:
            return self._finish_turn(event)

        # Part-level events: make sure a message/step is open (replay may start mid-turn).
        self._ensure_turn(event, chunks)
        pid = self._part_id(event)
        match event.type:
            case EventType.TEXT_START:
                self.open_text.add(pid)
                chunks.append(TextStartChunk(id=pid))
            case EventType.TEXT_DELTA:
                if pid not in self.open_text:
                    self.open_text.add(pid)
                    chunks.append(TextStartChunk(id=pid))
                chunks.append(TextDeltaChunk(id=pid, delta=event.delta or ""))
            case EventType.TEXT_END:
                self.open_text.discard(pid)
                chunks.append(TextEndChunk(id=pid))
            case EventType.REASONING_START:
                self.open_reasoning.add(pid)
                chunks.append(ReasoningStartChunk(id=pid))
            case EventType.REASONING_DELTA:
                if pid not in self.open_reasoning:
                    self.open_reasoning.add(pid)
                    chunks.append(ReasoningStartChunk(id=pid))
                chunks.append(ReasoningDeltaChunk(id=pid, delta=event.delta or ""))
            case EventType.REASONING_END:
                self.open_reasoning.discard(pid)
                chunks.append(ReasoningEndChunk(id=pid))
            case EventType.TOOL_INPUT_START:
                chunks.append(
                    ToolInputStartChunk(
                        tool_call_id=event.tool_call_id or "", tool_name=event.tool_name or ""
                    )
                )
            case EventType.TOOL_INPUT_DELTA:
                chunks.append(
                    ToolInputDeltaChunk(
                        tool_call_id=event.tool_call_id or "", input_text_delta=event.delta or ""
                    )
                )
            case EventType.TOOL_INPUT_AVAILABLE:
                chunks.append(
                    ToolInputAvailableChunk(
                        tool_call_id=event.tool_call_id or "",
                        tool_name=event.tool_name or "",
                        input=event.args if event.args is not None else {},
                    )
                )
            case EventType.TOOL_OUTPUT:
                if event.error is not None:
                    chunks.append(
                        ToolOutputErrorChunk(
                            tool_call_id=event.tool_call_id or "", error_text=event.error
                        )
                    )
                else:
                    chunks.append(
                        ToolOutputAvailableChunk(
                            tool_call_id=event.tool_call_id or "", output=event.output
                        )
                    )
            case EventType.USAGE_UPDATED:
                chunks.append(MessageMetadataChunk(message_metadata={"usage": event.usage}))
            case EventType.REQUEST_RESOLVED:
                chunks.append(
                    DataChunk(
                        type=DataPart.REQUEST_RESOLVED,
                        data={"approvals": event.approvals or []},
                        id=f"resolved:{event.turn_id}",
                    )
                )
            case _:  # pragma: no cover - exhaustive above
                pass
        return chunks

    # --- helpers -----------------------------------------------------------------------------

    def _part_id(self, event: StreamEvent) -> str:
        return f"{event.turn_id}:{event.attempt}:{event.part_id or event.tool_call_id or 'p'}"

    def _begin_turn(self, event: StreamEvent, chunks: list[BaseChunk]) -> None:
        self.turn_id = event.turn_id
        self.attempt = event.attempt
        self.open_text.clear()
        self.open_reasoning.clear()
        chunks.append(
            StartChunk(
                message_id=event.turn_id,
                message_metadata={
                    "turnId": event.turn_id,
                    "clientMessageId": event.client_message_id,
                },
            )
        )
        chunks.append(StartStepChunk())
        self.step_open = True

    def _ensure_turn(self, event: StreamEvent, chunks: list[BaseChunk]) -> None:
        if self.turn_id != event.turn_id:
            self._begin_turn(event, chunks)
            return
        if event.attempt > self.attempt:
            # A retried model activity: the previous attempt's partial output is superseded.
            self._close_open_parts(chunks)
            self.attempt = event.attempt
            chunks.append(
                DataChunk(
                    type=DataPart.RETRY,
                    data={"turnId": event.turn_id, "attempt": event.attempt},
                    id=f"retry:{event.turn_id}",
                )
            )
        if not self.step_open:
            chunks.append(StartStepChunk())
            self.step_open = True

    def _close_open_parts(self, chunks: list[BaseChunk]) -> None:
        for pid in sorted(self.open_text):
            chunks.append(TextEndChunk(id=pid))
        for pid in sorted(self.open_reasoning):
            chunks.append(ReasoningEndChunk(id=pid))
        self.open_text.clear()
        self.open_reasoning.clear()

    def _finish_turn(self, event: StreamEvent) -> list[BaseChunk]:
        chunks: list[BaseChunk] = []
        if self.turn_id != event.turn_id:
            self._begin_turn(event, chunks)
        self._close_open_parts(chunks)
        terminal = event.terminal or TurnTerminal.COMPLETED
        if event.error:
            chunks.append(ErrorChunk(error_text=event.error))
        if event.approvals:
            chunks.append(
                DataChunk(
                    type=DataPart.APPROVAL,
                    data={"turnId": event.turn_id, "approvals": event.approvals},
                    id=f"approval:{event.turn_id}",
                )
            )
        if self.step_open:
            chunks.append(FinishStepChunk())
            self.step_open = False
        chunks.append(
            FinishChunk(
                finish_reason=FINISH_REASONS[terminal],
                message_metadata={
                    "turnId": event.turn_id,
                    "terminal": terminal,
                    "status": event.status,
                    "usage": event.usage,
                },
            )
        )
        self.turn_id = None
        return chunks


def chunk_dicts(chunks: list[BaseChunk]) -> list[dict[str, Any]]:
    """Test helper: chunks as the JSON objects the client sees."""
    return [json.loads(c.encode(2)) for c in chunks]
