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

### Step 3: Redis and app Postgres

Compose adds `redis` (6379) and `app-postgres` (host port 5433, db/user/password `toy`), separate
from Temporal's own Postgres.

```bash
docker compose up -d
uv run python -m toy.ping   # redis ping: True / postgres: PostgreSQL 16...
```

### Step 4: agent-api

FastAPI transport that depends on two ports (`toy/ports.py`): `ConversationWorkflowClient`
(submit command, read state) and `EventSource` (tail a conversation's event stream). It knows
nothing about Temporal or Redis. Contracts: `toy/commands.py` (discriminated command union and
ack), `toy/state.py` (conversation state), `toy/events.py` (SDK-independent stream events and
their Redis field encoding).

| Route | Purpose |
| --- | --- |
| `POST /agent/commands` | `Command{conversation_id, client_message_id, payload}`; payload `kind` is one of `send, steer, context, edit, delete, reorder, send_now, stop, tool_results`. Returns `CommandAck`. |
| `GET /agent/conversations/{id}/state` | `ConversationState` (status, pending queue, approvals, counters); 404 if unknown. |
| `GET /agent/stream?conversation_id=&after=` | SSE. Replays events after `after` (or `Last-Event-ID`), then tails. SSE `id:` is the Redis stream id. `?format=raw` selects the JSON debug encoder once step 6 installs the Vercel encoder. |

```bash
uv run pytest tests/test_api.py           # dedup, queue edits, status guards, 422/404, SSE replay+tail
API_BACKEND=memory uv run agent-api       # port 8400, in-memory stub (no Temporal)
curl -s -X POST localhost:8400/agent/commands -H 'content-type: application/json' \
  -d '{"conversation_id":"c1","client_message_id":"m1","payload":{"kind":"send","content":"hello"}}'
curl -s localhost:8400/agent/conversations/c1/state
curl -N "localhost:8400/agent/stream?conversation_id=c1"
```

`API_BACKEND=temporal` (the default) uses the Temporal client from step 5 and the Redis event
source (`toy/adapters/redis_events.py`, `XADD` / `XRANGE` + `XREAD BLOCK`).
