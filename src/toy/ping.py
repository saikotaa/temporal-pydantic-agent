"""Step 3 validation: ping Redis and the app Postgres.

Run: `uv run python -m toy.ping`
"""

from __future__ import annotations

import asyncio

import asyncpg
import redis.asyncio as redis

from toy.settings import get_settings


async def main() -> None:
    settings = get_settings()
    r = redis.from_url(settings.redis_url)  # pyright: ignore[reportUnknownMemberType]
    try:
        pong = await r.ping()  # pyright: ignore[reportUnknownMemberType]
        print(f"redis ping: {pong}")
        assert pong is True
    finally:
        await r.aclose()

    conn = await asyncpg.connect(settings.database_url)
    try:
        version = await conn.fetchval("select version()")
        print(f"postgres: {version}")
        assert isinstance(version, str) and version.startswith("PostgreSQL")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
