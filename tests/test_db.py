"""PostgresRepository against the compose app Postgres (skipped if unreachable)."""

from __future__ import annotations

import socket
import uuid
from collections.abc import AsyncIterator

import pytest
from pydantic_ai.messages import ModelRequest, ModelResponse, TextPart, UserPromptPart

from toy.db import PostgresRepository, StatusRecord, TurnRecord, create_pool, init_schema
from toy.settings import get_settings
from toy.state import ConversationStatus, TurnTerminal


@pytest.fixture
async def repo() -> AsyncIterator[PostgresRepository]:
    url = get_settings().database_url
    try:
        with socket.create_connection(("localhost", 5433), timeout=1):
            pass
    except OSError:
        pytest.skip("app postgres not reachable on 5433")
    await init_schema(url)
    pool = await create_pool(url)
    try:
        yield PostgresRepository(pool)
    finally:
        await pool.close()


async def test_persist_and_load_roundtrip(repo: PostgresRepository) -> None:
    conv = f"db-{uuid.uuid4().hex[:8]}"
    messages = [
        ModelRequest(parts=[UserPromptPart(content="hi")]),
        ModelResponse(parts=[TextPart(content="hello")], model_name="test"),
    ]
    record = TurnRecord(conv, "turn-1", messages, TurnTerminal.COMPLETED, {"requests": 1})
    await repo.persist_turn(record)
    await repo.persist_turn(record)  # idempotent on retry
    loaded = await repo.load_history(conv)
    assert len(loaded) == 2
    assert isinstance(loaded[0], ModelRequest) and loaded[0].parts[0].content == "hi"  # type: ignore[union-attr]
    assert isinstance(loaded[1], ModelResponse) and loaded[1].model_name == "test"
    await repo.mirror_status(StatusRecord(conv, ConversationStatus.BLOCKED, 2, 1))
    await repo.mirror_status(StatusRecord(conv, ConversationStatus.IDLE, 0, 1))
