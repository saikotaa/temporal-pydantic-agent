"""Entrypoint: `uv run agent-api`. `API_BACKEND=memory|temporal` (default temporal), `PORT`."""

from __future__ import annotations

import asyncio
import logging
import os
from enum import StrEnum

import uvicorn

from toy.adapters.memory import InMemoryEventSource, InMemoryWorkflowClient
from toy.api.app import ApiDeps, create_app, json_sse_encoder
from toy.api.vercel_encoder import UI_STREAM_HEADERS, VercelUiStreamEncoder
from toy.ports import ConversationWorkflowClient, EventSource
from toy.settings import get_settings


class ApiBackend(StrEnum):
    MEMORY = "memory"
    TEMPORAL = "temporal"


RAW_FORMAT = "raw"


async def build_deps(backend: ApiBackend) -> ApiDeps:
    settings = get_settings()
    workflows: ConversationWorkflowClient
    events: EventSource
    if backend == ApiBackend.MEMORY:
        workflows, events = InMemoryWorkflowClient(), InMemoryEventSource()
    else:
        from toy.adapters.redis_events import RedisEventSource, make_redis
        from toy.adapters.temporal_client import connect_temporal_client

        workflows = await connect_temporal_client(settings)
        events = RedisEventSource(make_redis(settings.redis_url))
    return ApiDeps(
        workflows=workflows,
        events=events,
        encoder=VercelUiStreamEncoder,
        stream_headers=UI_STREAM_HEADERS,
        encoders={RAW_FORMAT: lambda: json_sse_encoder},
    )


async def serve(backend: ApiBackend, port: int) -> None:
    # Build the deps on the same loop uvicorn serves on (Temporal/Redis clients are loop-bound).
    deps = await build_deps(backend)
    app = create_app(deps)
    config = uvicorn.Config(app, host="0.0.0.0", port=port, log_level="info")
    await uvicorn.Server(config).serve()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    backend = ApiBackend(os.environ.get("API_BACKEND", ApiBackend.TEMPORAL))
    port = int(os.environ.get("PORT", "8400"))
    asyncio.run(serve(backend, port))


if __name__ == "__main__":
    main()
