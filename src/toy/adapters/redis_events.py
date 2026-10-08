"""Redis Stream implementation of the event channel (`XADD` in, `XRANGE`+`XREAD BLOCK` out)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any, cast

import redis.asyncio as redis

from toy.events import StreamEvent, stream_key

DEFAULT_MAXLEN = 10_000
DEFAULT_BLOCK_MS = 5_000
SOCKET_TIMEOUT_S = 30.0
"""redis-py 8 applies a 5 s default socket (read) timeout, which would kill an `XREAD BLOCK`
longer than that; the client is created with a timeout comfortably above the block time."""


class RedisEventPublisher:
    def __init__(self, client: redis.Redis, *, maxlen: int = DEFAULT_MAXLEN) -> None:
        self._redis = client
        self._maxlen = maxlen

    async def publish(self, event: StreamEvent) -> str:
        key = stream_key(event.conversation_id)
        fields = cast(dict[Any, Any], event.to_redis_fields())
        xadd = cast(Any, self._redis.xadd)
        stream_id = await xadd(key, fields, maxlen=self._maxlen, approximate=True)
        return stream_id.decode() if isinstance(stream_id, bytes) else str(stream_id)


class RedisEventSource:
    def __init__(self, client: redis.Redis, *, block_ms: int = DEFAULT_BLOCK_MS) -> None:
        self._redis = client
        self._block_ms = block_ms

    async def tail(
        self, conversation_id: str, after: str | None
    ) -> AsyncIterator[tuple[str, StreamEvent]]:
        key = stream_key(conversation_id)
        last = after or "0-0"
        # Replay everything after the cursor first (bounded pages), then block for new entries.
        xrange = cast(Any, self._redis.xrange)
        while True:
            entries = await xrange(key, min=f"({last}", max="+", count=500)
            if not entries:
                break
            for stream_id, fields in entries:
                last = _decode(stream_id)
                yield last, StreamEvent.from_redis_fields(fields)
        xread = cast(Any, self._redis.xread)
        while True:
            try:
                result = await xread({key: last}, block=self._block_ms, count=100)
            except TimeoutError:
                # Client-side read timeout racing the server's BLOCK expiry: nothing new.
                continue
            if not result:
                continue
            for _key, entries in result:
                for stream_id, fields in entries:
                    last = _decode(stream_id)
                    yield last, StreamEvent.from_redis_fields(fields)


def _decode(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def make_redis(url: str, *, socket_timeout: float = SOCKET_TIMEOUT_S) -> redis.Redis:
    return cast(redis.Redis, cast(Any, redis.Redis).from_url(url, socket_timeout=socket_timeout))
