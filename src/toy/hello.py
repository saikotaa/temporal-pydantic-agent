"""Step 2 validation: a hello workflow round trip against the compose Temporal server.

Run: `uv run python -m toy.hello`
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.worker import Worker

from toy.settings import get_settings


@activity.defn
async def say_hello(name: str) -> str:
    return f"hello, {name}"


@workflow.defn
class HelloWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        return await workflow.execute_activity(
            say_hello, name, start_to_close_timeout=timedelta(seconds=10)
        )


async def main() -> None:
    settings = get_settings()
    client = await Client.connect(settings.temporal_address, namespace=settings.temporal_namespace)
    task_queue = "toy-hello"
    async with Worker(
        client, task_queue=task_queue, workflows=[HelloWorkflow], activities=[say_hello]
    ):
        result = await client.execute_workflow(
            HelloWorkflow.run,
            "temporal",
            id=f"hello-{uuid.uuid4()}",
            task_queue=task_queue,
        )
    assert result == "hello, temporal", result
    print(result)


if __name__ == "__main__":
    asyncio.run(main())
