"""Step 10: measure Temporal history and payload sizes for a 50-turn conversation, with and
without the claim-check codec.

Runs `ConversationWorkflow` against the compose Temporal server with an in-process worker and a
FunctionModel that answers with ~2 KB of text per turn (a typical assistant reply), so the
message history handed to every model activity grows by roughly 2.5 KB per turn.

    uv run python scripts/measure_claim_check.py [--turns 50] [--reply-bytes 2000]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path

from pydantic_ai.durable_exec.temporal import PydanticAIPlugin
from pydantic_ai.messages import ModelMessage
from pydantic_ai.models.function import AgentInfo, DeltaToolCalls, FunctionModel
from temporalio.client import Client
from temporalio.worker import Worker

from toy.activities import ConversationActivities
from toy.adapters.memory import InMemoryEventSource
from toy.adapters.temporal_client import TemporalWorkflowClient
from toy.claim_check import DEFAULT_THRESHOLD, ClaimCheckCodec, ClaimCheckPlugin, InMemoryClaimStore
from toy.commands import Command
from toy.db import InMemoryRepository
from toy.runtime import runtime
from toy.settings import get_settings
from toy.workflow import ConversationWorkflow, WorkflowInput


@dataclass
class Measurement:
    label: str
    turns: int
    events: int
    history_bytes: int
    max_payload_bytes: int
    payloads_over_threshold: int
    offloaded_objects: int = 0
    offloaded_original_bytes: int = 0
    offloaded_stored_bytes: int = 0


def reply_model(reply_bytes: int) -> FunctionModel:
    filler = ("Lorem ipsum dolor sit amet, consectetur adipiscing elit. " * 400)[:reply_bytes]

    async def stream(
        messages: list[ModelMessage], info: AgentInfo
    ) -> AsyncIterator[str | DeltaToolCalls]:
        yield f"Turn reply ({len(messages)} messages in history). "
        yield filler

    return FunctionModel(stream_function=stream, model_name="measure")


async def run_conversation(
    client: Client, *, label: str, turns: int, reply_bytes: int, codec: ClaimCheckCodec | None
) -> Measurement:
    task_queue = f"toy-measure-{uuid.uuid4().hex[:8]}"
    conv = f"measure-{label}-{uuid.uuid4().hex[:6]}"
    events = InMemoryEventSource()
    runtime.model = reply_model(reply_bytes)
    runtime.publisher = events
    activities = ConversationActivities(InMemoryRepository(), events)
    workflows = TemporalWorkflowClient(client, task_queue)
    async with Worker(
        client, task_queue=task_queue, workflows=[ConversationWorkflow], activities=activities.all
    ):
        handle = await client.start_workflow(
            ConversationWorkflow.run,
            WorkflowInput(conversation_id=conv, turns_per_run=turns + 1),
            id=f"conv:{conv}",
            task_queue=task_queue,
        )
        for i in range(turns):
            await workflows.submit(
                Command.model_validate(
                    {
                        "conversation_id": conv,
                        "client_message_id": f"m{i}",
                        "payload": {"kind": "send", "content": f"message {i}: tell me more"},
                    }
                )
            )
        while True:
            state = await workflows.get_state(conv)
            if state and state.turn_count == turns and state.status == "idle":
                break
            await asyncio.sleep(0.5)
        history = await handle.fetch_history()
    sizes: list[int] = []
    for event in history.events:
        for _name, value in event.ListFields():
            attrs = getattr(value, "ListFields", None)
            if attrs is None:
                continue
            for _n, v in value.ListFields():
                payloads = getattr(v, "payloads", None)
                if payloads is not None:
                    sizes.extend(len(p.data) for p in payloads)
    m = Measurement(
        label=label,
        turns=turns,
        events=len(history.events),
        history_bytes=sum(e.ByteSize() for e in history.events),
        max_payload_bytes=max(sizes, default=0),
        payloads_over_threshold=sum(1 for s in sizes if s >= DEFAULT_THRESHOLD),
    )
    if codec is not None:
        m.offloaded_objects = codec.stats.offloaded
        m.offloaded_original_bytes = codec.stats.original_bytes
        m.offloaded_stored_bytes = codec.stats.stored_bytes
    return m


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--turns", type=int, default=50)
    parser.add_argument("--reply-bytes", type=int, default=2000)
    parser.add_argument("--out", default="scripts/claim_check_measurement.json")
    args = parser.parse_args()
    settings = get_settings()

    plain = await Client.connect(settings.temporal_address, plugins=[PydanticAIPlugin()])
    without = await run_conversation(
        plain, label="plain", turns=args.turns, reply_bytes=args.reply_bytes, codec=None
    )
    codec = ClaimCheckCodec(InMemoryClaimStore())
    with_plugin = await Client.connect(
        settings.temporal_address, plugins=[PydanticAIPlugin(), ClaimCheckPlugin(codec)]
    )
    with_codec = await run_conversation(
        with_plugin,
        label="claim-check",
        turns=args.turns,
        reply_bytes=args.reply_bytes,
        codec=codec,
    )
    results = [without.__dict__, with_codec.__dict__]
    for r in results:
        print(json.dumps(r))
    await asyncio.to_thread(Path(args.out).write_text, json.dumps(results, indent=1))
    print(
        f"history bytes: {without.history_bytes:,} -> {with_codec.history_bytes:,} "
        f"({with_codec.history_bytes / max(without.history_bytes, 1):.0%}); "
        f"max payload: {without.max_payload_bytes:,} -> {with_codec.max_payload_bytes:,}; "
        f"events: {without.events} / {with_codec.events}"
    )


if __name__ == "__main__":
    asyncio.run(main())
