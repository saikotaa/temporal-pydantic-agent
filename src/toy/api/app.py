"""agent-api routes. Knows nothing about Temporal or Redis; everything goes through ports."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass, field

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from toy.commands import Command, CommandAck
from toy.events import EventType, StreamEvent
from toy.ports import ConversationWorkflowClient, EventSource
from toy.state import ConversationState

log = logging.getLogger(__name__)

SseEncoder = Callable[[str, StreamEvent], bytes]
"""Turns one (stream id, event) pair into the bytes of one or more SSE frames."""

EncoderFactory = Callable[[], SseEncoder]
"""Builds a fresh encoder per SSE connection (encoders may be stateful)."""

SSE_HEADERS: Mapping[str, str] = {
    "cache-control": "no-cache",
    "x-accel-buffering": "no",
    "connection": "keep-alive",
}


def json_sse_encoder(stream_id: str, event: StreamEvent) -> bytes:
    """Debug encoder: one frame per event, data = the normalized event as JSON."""
    payload = event.model_dump(mode="json", exclude_none=True)
    return f"id: {stream_id}\nevent: {event.type}\ndata: {json.dumps(payload)}\n\n".encode()


def _json_encoder_factory() -> SseEncoder:
    return json_sse_encoder


@dataclass
class ApiDeps:
    workflows: ConversationWorkflowClient
    events: EventSource
    encoder: EncoderFactory = _json_encoder_factory
    stream_headers: Mapping[str, str] = field(default_factory=dict[str, str])
    encoders: Mapping[str, EncoderFactory] = field(default_factory=dict[str, EncoderFactory])
    """Optional alternative encoders selectable with `?format=`."""
    keepalive_seconds: float = 15.0


def create_app(deps: ApiDeps) -> FastAPI:
    app = FastAPI(title="agent-api", version="0.1.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["*"],
    )

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/agent/commands", response_model=CommandAck)
    async def post_command(command: Command) -> CommandAck:
        return await deps.workflows.submit(command)

    @app.get("/agent/conversations/{conversation_id}/state", response_model=ConversationState)
    async def get_state(conversation_id: str) -> ConversationState:
        state = await deps.workflows.get_state(conversation_id)
        if state is None:
            raise HTTPException(status_code=404, detail="conversation not found")
        return state

    @app.get("/agent/stream")
    async def stream(
        request: Request,
        conversation_id: str = Query(min_length=1),
        after: str | None = Query(default=None),
        format: str | None = Query(default=None),
        once: bool = Query(default=False, description="close after the next turn.completed"),
    ) -> StreamingResponse:
        factory = deps.encoder
        if format is not None:
            if format not in deps.encoders:
                raise HTTPException(status_code=422, detail=f"unknown format {format!r}")
            factory = deps.encoders[format]
        encoder = factory()
        # Last-Event-ID (EventSource reconnect) wins over the query parameter.
        last_id = request.headers.get("last-event-id")
        start_after = last_id or after

        async def body() -> AsyncIterator[bytes]:
            # The tail generator is consumed by its own task feeding a queue, so keepalive
            # timeouts never cancel (and thereby close) the underlying Redis read.
            queue: asyncio.Queue[tuple[str, StreamEvent] | None] = asyncio.Queue()
            tail = deps.events.tail(conversation_id, start_after)

            async def pump() -> None:
                try:
                    async for item in tail:
                        await queue.put(item)
                except Exception:
                    log.exception("event tail for %s failed", conversation_id)
                finally:
                    await queue.put(None)

            pump_task = asyncio.create_task(pump())
            try:
                while True:
                    try:
                        item = await asyncio.wait_for(queue.get(), timeout=deps.keepalive_seconds)
                    except TimeoutError:
                        if await request.is_disconnected():
                            return
                        yield b": keepalive\n\n"
                        continue
                    if item is None:
                        return
                    stream_id, event = item
                    yield encoder(stream_id, event)
                    if once and event.type == EventType.TURN_COMPLETED:
                        return
                    if await request.is_disconnected():
                        return
            finally:
                pump_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await pump_task

        headers = {**SSE_HEADERS, **deps.stream_headers}
        if format is not None:
            # Alternative encoders don't promise the default encoder's framing headers.
            headers = dict(SSE_HEADERS)
        return StreamingResponse(body(), media_type="text/event-stream", headers=headers)

    return app
