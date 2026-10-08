from __future__ import annotations

import asyncio
import socket
import uuid
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import Any

import pytest
from pydantic_ai.models import Model
from pydantic_ai.models.test import TestModel
from temporalio.client import Client
from temporalio.worker import Worker

from toy.activities import ConversationActivities
from toy.adapters.memory import InMemoryEventSource
from toy.adapters.temporal_client import TemporalWorkflowClient
from toy.db import InMemoryRepository
from toy.runtime import runtime
from toy.settings import get_settings
from toy.state import ConversationState
from toy.workflow import ConversationWorkflow


def _reachable(address: str) -> bool:
    host, _, port = address.partition(":")
    try:
        with socket.create_connection((host, int(port or 7233)), timeout=1):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def temporal_address() -> str:
    address = get_settings().temporal_address
    if not _reachable(address):
        pytest.skip(f"temporal not reachable at {address} (docker compose up -d)")
    return address


@pytest.fixture
async def temporal_client(temporal_address: str) -> Client:
    from pydantic_ai.durable_exec.temporal import PydanticAIPlugin

    return await Client.connect(temporal_address, plugins=[PydanticAIPlugin()])


@dataclass
class Harness:
    client: Client
    workflows: TemporalWorkflowClient
    repo: InMemoryRepository
    events: InMemoryEventSource
    task_queue: str
    conversation_id: str

    def set_model(self, model: Model) -> None:
        runtime.model = model

    async def state(self) -> ConversationState:
        state = await self.workflows.get_state(self.conversation_id)
        assert state is not None
        return state

    async def wait_for(
        self, predicate: Callable[[ConversationState], bool], *, timeout_s: float = 30
    ) -> ConversationState:
        state: ConversationState | None = None
        try:
            async with asyncio.timeout(timeout_s):
                while True:
                    state = await self.workflows.get_state(self.conversation_id)
                    if state is not None and predicate(state):
                        return state
                    await asyncio.sleep(0.1)
        except TimeoutError:
            raise AssertionError(f"timeout waiting for state; last={state}") from None

    async def wait_until(self, predicate: Callable[[], bool], *, timeout_s: float = 30) -> None:
        async with asyncio.timeout(timeout_s):
            while not predicate():  # noqa: ASYNC110 - polling an in-memory log is intended
                await asyncio.sleep(0.05)

    def event_types(self) -> list[str]:
        return [e.type.value for _, e in self.events.logs.get(self.conversation_id, [])]

    def events_of(self, type_: str) -> list[Any]:
        return [e for _, e in self.events.logs.get(self.conversation_id, []) if e.type == type_]


@pytest.fixture
async def harness(temporal_client: Client) -> AsyncIterator[Harness]:
    suffix = uuid.uuid4().hex[:8]
    task_queue = f"toy-test-{suffix}"
    repo = InMemoryRepository()
    events = InMemoryEventSource()
    runtime.model = TestModel(call_tools=[], custom_output_text="ok")
    runtime.publisher = events
    activities = ConversationActivities(repo, events)
    worker = Worker(
        temporal_client,
        task_queue=task_queue,
        workflows=[ConversationWorkflow],
        activities=activities.all,
    )
    async with worker:
        yield Harness(
            client=temporal_client,
            workflows=TemporalWorkflowClient(temporal_client, task_queue),
            repo=repo,
            events=events,
            task_queue=task_queue,
            conversation_id=f"t-{suffix}",
        )
    runtime.model = None
    runtime.publisher = None
