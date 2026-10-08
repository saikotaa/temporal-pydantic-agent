"""Step 9: drive the human-in-the-loop flow through agent-api and assert the outcome.

Run with the stack up (compose, `LLM_MODEL=test uv run agent-worker`, `uv run agent-api`):

    uv run python scripts/validate_hitl.py [--api http://localhost:8400]

With `LLM_MODEL=test` the Pydantic AI `TestModel` calls every tool, so the first turn always
asks for approval of `create_ticket`. With a real model, the prompt asks for a ticket explicitly.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import uuid
from typing import Any

import asyncpg
import httpx

from toy.adapters.redis_events import RedisEventSource, make_redis
from toy.events import EventType
from toy.settings import get_settings

PROMPT = "Please open a support ticket titled 'login bug' with body 'users cannot log in'."


class Api:
    def __init__(self, base: str, conv: str) -> None:
        self.http = httpx.AsyncClient(base_url=base, timeout=30)
        self.conv = conv

    async def command(self, payload: dict[str, Any]) -> dict[str, Any]:
        r = await self.http.post(
            "/agent/commands",
            json={
                "conversation_id": self.conv,
                "client_message_id": uuid.uuid4().hex,
                "payload": payload,
            },
        )
        r.raise_for_status()
        return r.json()

    async def state(self) -> dict[str, Any]:
        r = await self.http.get(f"/agent/conversations/{self.conv}/state")
        r.raise_for_status()
        return r.json()

    async def wait_for(self, predicate: Any, *, timeout_s: float = 60) -> dict[str, Any]:
        async with asyncio.timeout(timeout_s):
            while True:
                state = await self.state()
                if predicate(state):
                    return state
                await asyncio.sleep(0.2)


def check(cond: bool, msg: str) -> None:
    print(("  ok   " if cond else "  FAIL ") + msg)
    if not cond:
        raise SystemExit(1)


async def approval_round(api: Api, decision: str) -> None:
    expected_turns = 2
    print(f"[{decision}] send prompt")
    ack = await api.command({"kind": "send", "content": PROMPT})
    check(ack["accepted"], f"send accepted (workflow {ack['workflow_id']})")
    state = await api.wait_for(lambda s: s["status"] == "blocked")
    check(len(state["pending_approvals"]) == 1, "blocked with exactly one pending approval")
    approval = state["pending_approvals"][0]
    check(
        approval["tool_name"] == "create_ticket",
        f"approval is for create_ticket {approval['args']}",
    )
    check(state["last_terminal"] == "blocked", "last_terminal == blocked")

    print(f"[{decision}] resolve")
    ack = await api.command(
        {
            "kind": "tool_results",
            "results": [
                {
                    "tool_call_id": approval["tool_call_id"],
                    "decision": decision,
                    "reason": "validated by script",
                }
            ],
        }
    )
    check(ack["accepted"], "tool_results accepted")
    state = await api.wait_for(
        lambda s: s["turn_count"] == expected_turns and s["status"] == "idle"
    )
    check(state["pending_approvals"] == [], "no pending approvals after resolution")
    check(state["last_terminal"] == "completed", "resuming turn completed")


async def assert_postgres(conv: str, decision: str) -> None:
    conn = await asyncpg.connect(get_settings().database_url)
    try:
        rows = await conn.fetch(
            "select turn_id, terminal, count(*) as n from conversation_events "
            "where conversation_id = $1 group by turn_id, terminal order by turn_id",
            conv,
        )
        by_turn = {r["turn_id"]: (r["terminal"], r["n"]) for r in rows}
        check(
            by_turn.get("turn-1", ("",))[0] == "blocked",
            f"turn-1 persisted as blocked ({by_turn.get('turn-1')})",
        )
        check(
            by_turn.get("turn-2", ("",))[0] == "completed",
            f"turn-2 persisted as completed ({by_turn.get('turn-2')})",
        )
        content = await conn.fetchval(
            "select p->>'content' from conversation_events, "
            "jsonb_array_elements(message_json->'parts') p "
            "where conversation_id = $1 and p->>'part_kind' = 'tool-return' "
            "and p->>'tool_name' = 'create_ticket'",
            conv,
        )
        expected = "created" if decision == "accept" else "validated by script"
        check(
            content is not None and expected in content,
            f"create_ticket tool-return persisted with {expected!r}: {content!r}",
        )
        # The mirror activity runs just after the state query flips to idle; allow a moment.
        status = None
        for _ in range(50):
            status = await conn.fetchrow(
                "select status, turn_count from conversation_status where conversation_id = $1",
                conv,
            )
            if status is not None and status["status"] == "idle" and status["turn_count"] == 2:
                break
            await asyncio.sleep(0.2)
        check(
            status is not None and status["status"] == "idle" and status["turn_count"] == 2,
            f"status mirror idle / 2 turns ({dict(status) if status else None})",
        )
    finally:
        await conn.close()


async def assert_redis_sequence(conv: str, decision: str) -> None:
    redis = make_redis(get_settings().redis_url)
    try:
        events = []
        async with asyncio.timeout(10):
            try:
                async for _sid, ev in RedisEventSource(redis, block_ms=500).tail(conv, None):
                    events.append(ev)
                    if len([e for e in events if e.type == EventType.TURN_COMPLETED]) == 2:
                        break
            except TimeoutError:
                pass
    finally:
        await redis.aclose()
    kinds = [e.type.value for e in events]
    interesting = [
        k for k in kinds if k.startswith(("turn.", "request.")) or k == "tool.input_available"
    ]
    print("  events:", " ".join(interesting))
    # TestModel calls three tools in turn 1; a real model may call fewer, so compare by shape:
    # tool.input_available … turn.completed{blocked} … request.resolved … turn.completed{completed}
    compact = [k for k in interesting if k != "tool.input_available"]
    check(
        compact
        == ["turn.started", "turn.completed", "turn.started", "request.resolved", "turn.completed"],
        "turn/request event sequence",
    )
    check(
        "tool.input_available" in kinds
        and kinds.index("tool.input_available") < kinds.index("turn.completed"),
        "tool input precedes the blocked completion",
    )
    terminals = [
        e.terminal.value for e in events if e.type == EventType.TURN_COMPLETED and e.terminal
    ]
    check(terminals == ["blocked", "completed"], f"terminals {terminals}")
    resolved = [e for e in events if e.type == EventType.REQUEST_RESOLVED]
    check(
        bool(resolved and resolved[0].approvals)
        and resolved[0].approvals[0]["approved"] is (decision == "accept"),
        f"request.resolved carries approved={decision == 'accept'}",
    )
    outputs = [
        e for e in events if e.type == EventType.TOOL_OUTPUT and e.tool_name == "create_ticket"
    ]
    expected = "created" if decision == "accept" else "validated by script"
    check(
        len(outputs) == 1 and expected in str(outputs[0].output),
        "approved call ran" if decision == "accept" else "denied call returned the reason",
    )


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api", default="http://localhost:8400")
    args = parser.parse_args()
    # One conversation per decision: TestModel stops calling tools once the history holds tool
    # returns, so a second approval round in the same conversation would not block.
    for decision in ("accept", "decline"):
        conv = f"hitl-{decision}-{uuid.uuid4().hex[:6]}"
        api = Api(args.api, conv)
        print(f"conversation {conv}")
        await approval_round(api, decision)
        print("[postgres]")
        await assert_postgres(conv, decision)
        print("[redis]")
        await assert_redis_sequence(conv, decision)
        await api.http.aclose()
    print("HITL validation passed")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except SystemExit as e:
        sys.exit(e.code)
