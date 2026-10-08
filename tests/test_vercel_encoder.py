"""StreamEvent -> Vercel AI SDK UI message stream chunks."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from toy.api.vercel_encoder import VercelUiStreamEncoder, chunk_dicts
from toy.events import EventType, StreamEvent
from toy.state import ConversationStatus, TurnTerminal

FIXTURES = Path(__file__).parent / "fixtures"
CONV = "c1"


def ev(type_: EventType, turn_id: str = "turn-1", attempt: int = 1, **fields: Any) -> StreamEvent:
    return StreamEvent(conversation_id=CONV, turn_id=turn_id, attempt=attempt, type=type_, **fields)


def encode_all(events: list[StreamEvent]) -> list[dict[str, Any]]:
    enc = VercelUiStreamEncoder()
    out: list[dict[str, Any]] = []
    for e in events:
        out.extend(chunk_dicts(enc.encode(e)))
    return out


def types(chunks: list[dict[str, Any]]) -> list[str]:
    return [c["type"] for c in chunks]


def test_plain_text_turn() -> None:
    chunks = encode_all(
        [
            ev(EventType.TURN_STARTED, client_message_id="m1"),
            ev(EventType.TEXT_START, part_id="p0"),
            ev(EventType.TEXT_DELTA, part_id="p0", delta="Hel"),
            ev(EventType.TEXT_DELTA, part_id="p0", delta="lo"),
            ev(EventType.TEXT_END, part_id="p0"),
            ev(
                EventType.TURN_COMPLETED,
                terminal=TurnTerminal.COMPLETED,
                status=ConversationStatus.IDLE,
                usage={"requests": 1},
            ),
        ]
    )
    assert types(chunks) == [
        "start",
        "start-step",
        "text-start",
        "text-delta",
        "text-delta",
        "text-end",
        "finish-step",
        "finish",
    ]
    assert chunks[0]["messageId"] == "turn-1"
    assert chunks[2]["id"] == chunks[3]["id"] == "turn-1:1:p0"
    assert chunks[-1]["finishReason"] == "stop"
    assert chunks[-1]["messageMetadata"]["usage"] == {"requests": 1}


def test_reasoning_then_text_and_sse_framing() -> None:
    enc = VercelUiStreamEncoder()
    frames = b"".join(
        enc(f"{i}-0", e)
        for i, e in enumerate(
            [
                ev(EventType.TURN_STARTED),
                ev(EventType.REASONING_START, part_id="p0"),
                ev(EventType.REASONING_DELTA, part_id="p0", delta="think"),
                ev(EventType.REASONING_END, part_id="p0"),
                ev(EventType.TEXT_DELTA, part_id="p1", delta="answer"),
                ev(EventType.TURN_COMPLETED, terminal=TurnTerminal.COMPLETED),
            ],
            start=1,
        )
    )
    text = frames.decode()
    blocks = [b for b in text.split("\n\n") if b]
    assert blocks[0].startswith("id: 1-0\ndata: ")
    assert blocks[-1] == "id: 6-0\ndata: [DONE]"
    kinds = [json.loads(b.split("data: ", 1)[1])["type"] for b in blocks[:-1]]
    # text.delta without text.start opens the part; the unclosed text is ended at finish.
    assert kinds == [
        "start",
        "start-step",
        "reasoning-start",
        "reasoning-delta",
        "reasoning-end",
        "text-start",
        "text-delta",
        "text-end",
        "finish-step",
        "finish",
    ]


def test_parallel_tools_and_tool_error() -> None:
    chunks = encode_all(
        [
            ev(EventType.TURN_STARTED),
            ev(EventType.TOOL_INPUT_START, tool_name="lookup_weather", tool_call_id="c1"),
            ev(EventType.TOOL_INPUT_START, tool_name="read_note", tool_call_id="c2"),
            ev(EventType.TOOL_INPUT_DELTA, tool_call_id="c1", delta='{"city":'),
            ev(EventType.TOOL_INPUT_DELTA, tool_call_id="c2", delta='{"key":"x"}'),
            ev(EventType.TOOL_INPUT_DELTA, tool_call_id="c1", delta='"Oslo"}'),
            ev(
                EventType.TOOL_INPUT_AVAILABLE,
                tool_name="read_note",
                tool_call_id="c2",
                args={"key": "x"},
            ),
            ev(
                EventType.TOOL_INPUT_AVAILABLE,
                tool_name="lookup_weather",
                tool_call_id="c1",
                args={"city": "Oslo"},
            ),
            ev(EventType.TOOL_OUTPUT, tool_name="read_note", tool_call_id="c2", error="boom"),
            ev(
                EventType.TOOL_OUTPUT, tool_name="lookup_weather", tool_call_id="c1", output="sunny"
            ),
            ev(EventType.TURN_COMPLETED, terminal=TurnTerminal.COMPLETED),
        ]
    )
    assert types(chunks)[2:10] == [
        "tool-input-start",
        "tool-input-start",
        "tool-input-delta",
        "tool-input-delta",
        "tool-input-delta",
        "tool-input-available",
        "tool-input-available",
        "tool-output-error",
    ]
    assert chunks[9]["errorText"] == "boom" and chunks[9]["toolCallId"] == "c2"
    assert chunks[10] == {"type": "tool-output-available", "toolCallId": "c1", "output": "sunny"}
    assert chunks[7]["input"] == {"key": "x"}


def test_approval_blocks_then_resolves() -> None:
    approvals = [{"tool_call_id": "c9", "tool_name": "create_ticket", "args": {"title": "t"}}]
    chunks = encode_all(
        [
            ev(EventType.TURN_STARTED),
            ev(
                EventType.TOOL_INPUT_AVAILABLE,
                tool_name="create_ticket",
                tool_call_id="c9",
                args={"title": "t"},
            ),
            ev(
                EventType.TURN_COMPLETED,
                terminal=TurnTerminal.BLOCKED,
                status=ConversationStatus.BLOCKED,
                approvals=approvals,
            ),
            ev(EventType.STATUS_CHANGED, status=ConversationStatus.BLOCKED, pending_count=0),
            ev(EventType.TURN_STARTED, turn_id="turn-2"),
            ev(
                EventType.REQUEST_RESOLVED,
                turn_id="turn-2",
                approvals=[{"tool_call_id": "c9", "approved": True}],
            ),
            ev(EventType.TOOL_OUTPUT, turn_id="turn-2", tool_call_id="c9", output="ok"),
            ev(EventType.TURN_COMPLETED, turn_id="turn-2", terminal=TurnTerminal.COMPLETED),
        ]
    )
    assert types(chunks) == [
        "start",
        "start-step",
        "tool-input-available",
        "data-approval",
        "finish-step",
        "finish",
        "data-status",
        "data-queue",
        "start",
        "start-step",
        "data-request-resolved",
        "tool-output-available",
        "finish-step",
        "finish",
    ]
    assert chunks[3]["data"]["approvals"] == approvals
    assert chunks[5]["finishReason"] == "other"
    assert chunks[6]["data"] == {"status": "blocked", "turnId": "turn-1"}
    assert chunks[10]["data"]["approvals"][0]["approved"] is True


def test_stop_and_failure_finish_reasons() -> None:
    chunks = encode_all(
        [
            ev(EventType.TURN_STARTED),
            ev(EventType.TEXT_DELTA, part_id="p0", delta="par"),
            ev(EventType.TURN_COMPLETED, terminal=TurnTerminal.STOPPED),
            ev(EventType.TURN_STARTED, turn_id="turn-2"),
            ev(
                EventType.TURN_COMPLETED,
                turn_id="turn-2",
                terminal=TurnTerminal.FAILED,
                error="kaboom",
            ),
        ]
    )
    assert types(chunks) == [
        "start", "start-step", "text-start", "text-delta", "text-end", "finish-step", "finish",
        "start", "start-step", "error", "finish-step", "finish",
    ]  # fmt: skip
    assert chunks[6]["finishReason"] == "other"
    assert chunks[9]["errorText"] == "kaboom" and chunks[11]["finishReason"] == "error"


def test_retry_bumps_attempt_and_closes_parts() -> None:
    chunks = encode_all(
        [
            ev(EventType.TURN_STARTED),
            ev(EventType.TEXT_DELTA, part_id="p0", delta="first try"),
            ev(EventType.TEXT_DELTA, part_id="p0", delta="second try", attempt=2),
            ev(EventType.TEXT_END, part_id="p0", attempt=2),
            ev(EventType.TURN_COMPLETED, terminal=TurnTerminal.COMPLETED, attempt=2),
        ]
    )
    assert types(chunks) == [
        "start", "start-step", "text-start", "text-delta", "text-end", "data-retry",
        "text-start", "text-delta", "text-end", "finish-step", "finish",
    ]  # fmt: skip
    assert chunks[2]["id"] == "turn-1:1:p0" and chunks[6]["id"] == "turn-1:2:p0"
    assert chunks[5]["data"] == {"turnId": "turn-1", "attempt": 2}


def test_replay_starting_mid_turn_opens_message() -> None:
    chunks = encode_all(
        [
            ev(EventType.TEXT_DELTA, part_id="p0", delta="late"),
            ev(EventType.TURN_COMPLETED, terminal=TurnTerminal.COMPLETED),
        ]
    )
    assert types(chunks) == [
        "start", "start-step", "text-start", "text-delta", "text-end", "finish-step", "finish",
    ]  # fmt: skip


def test_recorded_hitl_fixture() -> None:
    recorded = json.loads((FIXTURES / "hitl_recorded.json").read_text())
    enc = VercelUiStreamEncoder()
    frames = b"".join(enc(r["id"], StreamEvent.model_validate(r["event"])) for r in recorded)
    text = frames.decode()
    assert text.count("data: [DONE]") == 2
    datas = [b.split("data: ", 1)[1] for b in text.split("\n\n") if b]
    chunks = [json.loads(d) for d in datas if d != "[DONE]"]
    kinds = types(chunks)
    assert kinds.count("start") == 2 and kinds.count("finish") == 2
    assert "data-approval" in kinds and "data-request-resolved" in kinds
    assert kinds.index("data-approval") < kinds.index("data-request-resolved")
    finishes = [c for c in chunks if c["type"] == "finish"]
    assert [f["finishReason"] for f in finishes] == ["other", "stop"]
    assert all(c["id"].startswith("turn-2:1:") for c in chunks if c["type"].startswith("text-"))
    # Every SSE frame carries the Redis stream id as its SSE id.
    assert all(b.startswith("id: 1791") for b in text.split("\n\n") if b)
