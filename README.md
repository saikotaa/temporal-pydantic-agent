# pydantic-temporal-toy

Toy of the target agent runtime for sf-platform, proven end to end before porting:

- **Pydantic AI** agent made durable with Temporal (`TemporalDurability`), one Temporal
  workflow per conversation holding an editable command queue.
- Model and tool calls run as Temporal activities; token streaming goes through
  `event_stream_handler` into a **Redis Stream**.
- **FastAPI** `agent-api` accepts commands, tails Redis and serves SSE as Vercel AI SDK
  UI message stream chunks.
- Turns persisted to **Postgres** as Pydantic `ModelMessage` JSON.
- Human-in-the-loop via Pydantic AI deferred tools (run ends blocked, resumes on a
  `tool_results` command).
- Per-model-call billing via a `WrapperModel` that meters usage idempotently.
- Claim-check `PayloadCodec` to S3/MinIO for large payloads.

Everything runs locally with docker compose. No auth. Not production code.

## Prerequisites

- [mise](https://mise.jdx.dev) (python 3.13, node 22, uv pinned in `mise.toml`)
- Docker with compose v2
- An LLM key for live runs (see `.env.example`); tests use Pydantic AI `TestModel` and
  need no key.

## Steps

Each step below ends with a validation and a commit.

### Step 1: scaffold

```bash
mise install
uv sync
uv run ruff check . && uv run pyright && uv run pytest
```

Pinned by resolution at scaffold time: `pydantic-ai-slim 2.54`, `temporalio 1.34`,
`pydantic 2.13`, `fastapi 0.142`, `redis 8.1`.

### Step 2: Temporal in compose

`docker-compose.yml` runs `temporalio/auto-setup` with its own Postgres and the Temporal UI on
http://localhost:8233. The dynamic config in `docker/temporal/` pins the default blob/history
limits explicitly (used by step 10).

```bash
docker compose up -d
docker compose exec temporal temporal operator cluster health --address temporal:7233  # SERVING
uv run python -m toy.hello   # hello, temporal
```

The Temporal CLI is used through `docker compose exec temporal temporal ...`.
