"""ClaimCheckCodec: threshold, content addressing, round trip, missing object, plugin wiring."""

from __future__ import annotations

import uuid
from datetime import timedelta

import pytest
from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.converter import DataConverter
from temporalio.exceptions import ApplicationError
from temporalio.worker import Worker

from toy.claim_check import (
    ENCODING,
    METADATA_ENCODING,
    METADATA_KEY,
    ClaimCheckCodec,
    ClaimCheckPlugin,
    InMemoryClaimStore,
)


async def test_small_payloads_stay_inline_and_large_ones_round_trip() -> None:
    store = InMemoryClaimStore()
    codec = ClaimCheckCodec(store, threshold=1024)
    converter = DataConverter(payload_codec=codec)
    small = {"text": "x" * 100}
    big = {"text": "y" * 5000, "n": list(range(200))}
    encoded = await converter.encode([small, big])
    assert encoded[0].metadata.get(METADATA_ENCODING) != ENCODING
    assert encoded[1].metadata[METADATA_ENCODING] == ENCODING
    assert len(encoded[1].data) == 64  # sha256 hex
    assert store.puts == 1 and codec.stats.offloaded == 1 and codec.stats.inline == 1
    assert codec.stats.stored_bytes < codec.stats.original_bytes
    decoded = await converter.decode(encoded, type_hints=[dict, dict])
    assert decoded == [small, big]


async def test_content_addressed_dedup_and_missing_object() -> None:
    store = InMemoryClaimStore()
    codec = ClaimCheckCodec(store, threshold=10)
    converter = DataConverter(payload_codec=codec)
    payload = {"text": "same content " * 10}
    a = await converter.encode([payload])
    b = await converter.encode([payload])
    assert a[0].metadata[METADATA_KEY] == b[0].metadata[METADATA_KEY]
    assert store.puts == 1, "identical content is stored once"
    store.objects.clear()
    with pytest.raises(ApplicationError) as exc:
        await converter.decode(a, type_hints=[dict])
    assert exc.value.non_retryable and exc.value.type == "ClaimCheckMissing"


@activity.defn
async def echo_big(text: str) -> str:
    return text + "!"


@workflow.defn
class EchoWorkflow:
    @workflow.run
    async def run(self, text: str) -> str:
        return await workflow.execute_activity(
            echo_big, text, start_to_close_timeout=timedelta(seconds=10)
        )


async def test_plugin_keeps_pydantic_converter_and_offloads_through_temporal(
    temporal_address: str,
) -> None:
    from pydantic_ai.durable_exec.temporal import PydanticAIPlugin

    store = InMemoryClaimStore()
    codec = ClaimCheckCodec(store, threshold=20 * 1024)
    client = await Client.connect(
        temporal_address, plugins=[PydanticAIPlugin(), ClaimCheckPlugin(codec)]
    )
    assert client.data_converter.payload_codec is codec
    assert client.data_converter.payload_converter_class.__name__ == "PydanticAIPayloadConverter"
    task_queue = f"toy-cc-{uuid.uuid4().hex[:8]}"
    big = "z" * 100_000
    async with Worker(
        client, task_queue=task_queue, workflows=[EchoWorkflow], activities=[echo_big]
    ):
        handle = await client.start_workflow(
            EchoWorkflow.run, big, id=f"cc-{uuid.uuid4().hex[:8]}", task_queue=task_queue
        )
        result = await handle.result()
    assert result == big + "!"
    # input, activity input, activity result, workflow result: all over the threshold
    assert codec.stats.offloaded >= 4 and len(store.objects) >= 2
    history = await handle.fetch_history()
    raw = history.to_json()
    assert big not in raw, "the history holds claims, not the payload"
    assert "claim-check" in raw
