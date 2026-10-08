"""Postgres persistence for turns and a status mirror. `uv run python -m toy.db init` creates
the schema. The repository is a small protocol so the workflow tests can use memory."""

from __future__ import annotations

import asyncio
import json
import sys
from dataclasses import dataclass, field
from typing import Any, Protocol

import asyncpg
from pydantic_ai.messages import ModelMessage, ModelMessagesTypeAdapter

from toy.settings import get_settings
from toy.state import ConversationStatus, TurnTerminal

SCHEMA = """
create table if not exists conversation_events (
    conversation_id text not null,
    turn_id         text not null,
    seq             integer not null,
    message_json    jsonb not null,
    terminal        text not null,
    usage           jsonb,
    created_at      timestamptz not null default now(),
    primary key (conversation_id, turn_id, seq)
);
create index if not exists conversation_events_conv_idx
    on conversation_events (conversation_id, created_at, turn_id, seq);
create table if not exists conversation_status (
    conversation_id text primary key,
    status          text not null,
    pending_count   integer not null default 0,
    turn_count      integer not null default 0,
    updated_at      timestamptz not null default now()
);
"""


@dataclass
class TurnRecord:
    conversation_id: str
    turn_id: str
    messages: list[ModelMessage]
    terminal: TurnTerminal
    usage: dict[str, Any] | None = None


@dataclass
class StatusRecord:
    conversation_id: str
    status: ConversationStatus
    pending_count: int
    turn_count: int


class ConversationRepository(Protocol):
    async def load_history(self, conversation_id: str) -> list[ModelMessage]: ...

    async def persist_turn(self, record: TurnRecord) -> None: ...

    async def mirror_status(self, record: StatusRecord) -> None: ...


@dataclass
class InMemoryRepository:
    turns: list[TurnRecord] = field(default_factory=list[TurnRecord])
    statuses: dict[str, StatusRecord] = field(default_factory=dict[str, StatusRecord])

    async def load_history(self, conversation_id: str) -> list[ModelMessage]:
        out: list[ModelMessage] = []
        for t in self.turns:
            if t.conversation_id == conversation_id:
                out.extend(t.messages)
        return out

    async def persist_turn(self, record: TurnRecord) -> None:
        self.turns = [
            t
            for t in self.turns
            if not (t.conversation_id == record.conversation_id and t.turn_id == record.turn_id)
        ]
        self.turns.append(record)

    async def mirror_status(self, record: StatusRecord) -> None:
        self.statuses[record.conversation_id] = record


class PostgresRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def load_history(self, conversation_id: str) -> list[ModelMessage]:
        rows = await self._pool.fetch(
            "select message_json from conversation_events where conversation_id = $1 "
            "order by created_at, turn_id, seq",
            conversation_id,
        )
        out: list[ModelMessage] = []
        for row in rows:
            raw = row["message_json"]
            payload = raw if isinstance(raw, str) else json.dumps(raw)
            out.extend(ModelMessagesTypeAdapter.validate_json(f"[{payload}]"))
        return out

    async def persist_turn(self, record: TurnRecord) -> None:
        # One row per ModelMessage, Pydantic JSON. Idempotent on retry via the primary key.
        async with self._pool.acquire() as conn, conn.transaction():
            await conn.execute(
                "delete from conversation_events where conversation_id = $1 and turn_id = $2",
                record.conversation_id,
                record.turn_id,
            )
            for seq, message in enumerate(record.messages):
                message_json = ModelMessagesTypeAdapter.dump_json([message]).decode()[1:-1]
                await conn.execute(
                    "insert into conversation_events "
                    "(conversation_id, turn_id, seq, message_json, terminal, usage) "
                    "values ($1, $2, $3, $4::jsonb, $5, $6::jsonb)",
                    record.conversation_id,
                    record.turn_id,
                    seq,
                    message_json,
                    record.terminal.value,
                    json.dumps(record.usage) if record.usage is not None else None,
                )

    async def mirror_status(self, record: StatusRecord) -> None:
        await self._pool.execute(
            "insert into conversation_status (conversation_id, status, pending_count, turn_count) "
            "values ($1, $2, $3, $4) on conflict (conversation_id) do update set "
            "status = excluded.status, pending_count = excluded.pending_count, "
            "turn_count = excluded.turn_count, updated_at = now()",
            record.conversation_id,
            record.status.value,
            record.pending_count,
            record.turn_count,
        )


async def init_schema(database_url: str) -> None:
    conn = await asyncpg.connect(database_url)
    try:
        await conn.execute(SCHEMA)
    finally:
        await conn.close()


async def create_pool(database_url: str) -> asyncpg.Pool:
    pool = await asyncpg.create_pool(database_url, min_size=1, max_size=10)
    assert pool is not None
    return pool


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] != "init":
        print("usage: python -m toy.db init", file=sys.stderr)
        raise SystemExit(2)
    asyncio.run(init_schema(get_settings().database_url))
    print("schema ready")


if __name__ == "__main__":
    main()
