"""agent-api over the in-memory ports: command semantics, state, SSE replay + tail."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest

from toy.adapters.memory import InMemoryEventSource, InMemoryWorkflowClient
from toy.api.app import ApiDeps, create_app
from toy.events import EventType, StreamEvent
from toy.state import ConversationStatus, TurnTerminal

CONV = "c1"


@pytest.fixture
def workflows() -> InMemoryWorkflowClient:
    return InMemoryWorkflowClient()


@pytest.fixture
def events() -> InMemoryEventSource:
    return InMemoryEventSource()


@pytest.fixture
async def client(
    workflows: InMemoryWorkflowClient, events: InMemoryEventSource
) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(ApiDeps(workflows=workflows, events=events, keepalive_seconds=0.2))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


def cmd(kind: str, cmid: str, **payload: Any) -> dict[str, Any]:
    return {
        "conversation_id": CONV,
        "client_message_id": cmid,
        "payload": {"kind": kind, **payload},
    }


async def test_send_dedups_by_client_message_id(client: httpx.AsyncClient) -> None:
    r1 = await client.post("/agent/commands", json=cmd("send", "m1", content="hi"))
    r2 = await client.post("/agent/commands", json=cmd("send", "m1", content="hi again"))
    assert r1.status_code == 200 and r1.json()["accepted"] and not r1.json()["duplicate"]
    assert r2.json()["duplicate"] is True
    assert r1.json()["workflow_id"] == "conv:c1"
    state = (await client.get(f"/agent/conversations/{CONV}/state")).json()
    assert [m["client_message_id"] for m in state["pending"]] == ["m1"]
    assert state["pending"][0]["content"] == "hi"
    assert state["status"] == "idle"


async def test_edit_delete_reorder_send_now(client: httpx.AsyncClient) -> None:
    for i in (1, 2, 3):
        await client.post("/agent/commands", json=cmd("send", f"m{i}", content=f"msg {i}"))
    r = await client.post(
        "/agent/commands",
        json=cmd("edit", "e1", target_client_message_id="m2", content="edited"),
    )
    assert r.json()["accepted"]
    r = await client.post("/agent/commands", json=cmd("reorder", "r1", order=["m3", "m1"]))
    assert r.json()["accepted"]
    pending = (await client.get(f"/agent/conversations/{CONV}/state")).json()["pending"]
    assert [m["client_message_id"] for m in pending] == ["m3", "m1", "m2"]
    assert pending[2]["content"] == "edited"
    r = await client.post(
        "/agent/commands", json=cmd("send_now", "s1", target_client_message_id="m2")
    )
    assert r.json()["accepted"]
    r = await client.post(
        "/agent/commands", json=cmd("delete", "d1", target_client_message_id="m3")
    )
    assert r.json()["accepted"]
    pending = (await client.get(f"/agent/conversations/{CONV}/state")).json()["pending"]
    assert [m["client_message_id"] for m in pending] == ["m2", "m1"]
    r = await client.post(
        "/agent/commands", json=cmd("delete", "d2", target_client_message_id="zz")
    )
    assert r.json()["accepted"] is False


async def test_stop_and_tool_results_status_guards(
    client: httpx.AsyncClient, workflows: InMemoryWorkflowClient
) -> None:
    await client.post("/agent/commands", json=cmd("send", "m1", content="hi"))
    r = await client.post("/agent/commands", json=cmd("stop", "st1"))
    assert r.json()["accepted"] is False and r.json()["detail"] == "no turn running"
    results = [{"tool_call_id": "call_1", "decision": "accept"}]
    r = await client.post("/agent/commands", json=cmd("tool_results", "t1", results=results))
    assert r.json()["accepted"] is False and r.json()["detail"] == "not blocked"

    workflows.states[CONV].status = ConversationStatus.WORKING
    r = await client.post("/agent/commands", json=cmd("stop", "st2"))
    assert r.json()["accepted"] is True
    workflows.states[CONV].status = ConversationStatus.BLOCKED
    r = await client.post("/agent/commands", json=cmd("tool_results", "t2", results=results))
    assert r.json()["accepted"] is True
    pending = (await client.get(f"/agent/conversations/{CONV}/state")).json()["pending"]
    assert pending[0]["kind"] == "tool_results" and pending[0]["tool_results"][0]["approved"]


async def test_validation_and_not_found(client: httpx.AsyncClient) -> None:
    r = await client.post("/agent/commands", json=cmd("bogus", "x"))
    assert r.status_code == 422
    r = await client.post("/agent/commands", json=cmd("send", "x", content=""))
    assert r.status_code == 422
    r = await client.get("/agent/conversations/nope/state")
    assert r.status_code == 404
    r = await client.post("/agent/commands", json=cmd("stop", "x"))
    assert r.status_code == 200 and r.json()["accepted"] is False
    r = await client.get("/agent/stream", params={"conversation_id": CONV, "format": "nope"})
    assert r.status_code == 422


def _frames(raw: str) -> list[tuple[str, dict[str, Any]]]:
    out: list[tuple[str, dict[str, Any]]] = []
    for block in raw.split("\n\n"):
        sid, data = None, None
        for line in block.splitlines():
            if line.startswith("id: "):
                sid = line[4:]
            elif line.startswith("data: "):
                data = json.loads(line[6:])
        if sid and data is not None:
            out.append((sid, data))
    return out


async def test_sse_replays_then_tails(
    client: httpx.AsyncClient, events: InMemoryEventSource
) -> None:
    ev = StreamEvent(conversation_id=CONV, turn_id="t1", type=EventType.TURN_STARTED)
    first = await events.publish(ev)
    await events.publish(ev.model_copy(update={"type": EventType.TEXT_DELTA, "delta": "he"}))

    collected: list[tuple[str, dict[str, Any]]] = []

    async def consume() -> None:
        async with client.stream(
            "GET", "/agent/stream", params={"conversation_id": CONV, "after": first}
        ) as resp:
            assert resp.status_code == 200
            assert resp.headers["content-type"].startswith("text/event-stream")
            buf = ""
            async for chunk in resp.aiter_text():
                buf += chunk
                collected[:] = _frames(buf)
                if any(d["type"] == "turn.completed" for _, d in collected):
                    return

    task = asyncio.create_task(consume())
    await asyncio.sleep(0.1)
    await events.publish(
        ev.model_copy(update={"type": EventType.TURN_COMPLETED, "terminal": TurnTerminal.COMPLETED})
    )
    await asyncio.wait_for(task, timeout=5)
    assert [d["type"] for _, d in collected] == ["text.delta", "turn.completed"]
    assert collected[0][0] == "2-0"
