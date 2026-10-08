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

### Step 5: workflow, worker, and agent

One `ConversationWorkflow` per conversation (`toy/workflow.py`, workflow id `conv:{id}`), holding
the editable queue in workflow memory. The Pydantic AI agent (`toy/agent.py`) runs *inside* the
workflow with `TemporalDurability`: every model request and tool call is an activity, and the
`event_stream_handler` runs inside the model activity, normalizing Pydantic AI events into
`StreamEvent`s and `XADD`ing them to `conv:{id}:events` (`toy/stream_events.py`). Our own
activities (`toy/activities.py`) do the remaining I/O: `load_history`, `persist_turn` (one
Postgres row per `ModelMessage`, Pydantic JSON), `mirror_status`, `publish_event`.

| Command | Delivery | Effect |
| --- | --- | --- |
| `send`, `steer`, `context` | update-with-start (`USE_EXISTING`) | dedup by `client_message_id`; `send` appends to `pending`; steer/context go to the running turn's `inbox` (injected as user parts before the next model request by a history processor) or to `pending` when idle |
| `tool_results` | update on the existing workflow | only while `blocked`; goes to the head of `pending` |
| `edit`, `delete`, `reorder`, `send_now`, `stop` | signals | mutate `pending`; `stop` sets the cancel flag (ignored if `expected_turn_id` is not the running turn) |
| state | query | `ConversationState` |

The turn loop pops the head of `pending`, races the agent run against the cancel flag with
`workflow.wait`, persists, publishes `turn.completed{terminal}` and continues-as-new every
20 turns carrying `pending`, counters and seen ids. A run ending with `DeferredToolRequests`
(the `create_ticket` tool has `requires_approval=True`) sets `status=blocked` and records
`pending_approvals`; nothing holds a worker slot while blocked.

```bash
uv run python -m toy.db init                     # schema in app Postgres
LLM_MODEL=test uv run agent-worker               # Pydantic AI TestModel; no key needed
uv run agent-api                                  # API_BACKEND=temporal is the default
uv run pytest tests/test_workflow.py             # needs docker compose up (uses the real server)
```

With a real model set `LLM_MODEL=anthropic/claude-sonnet-5-5` plus `LITELLM_BASE_URL` /
`LITELLM_API_KEY` (LiteLLM proxy, OpenAI-compatible) in `.env`.

Mismatches found against the installed SDKs (pydantic-ai-slim 2.54, temporalio 1.34):

- The agent is registered on the workflow class via `__pydantic_ai_agents__` (base class
  `PydanticAIWorkflow`), with `TemporalDurability` passed in `Agent(capabilities=[...])`;
  `TemporalAgent` is deprecated.
- `history_processors` is now the `ProcessHistory` capability; it runs in workflow code and
  reads the inbox through `workflow.instance()`.
- `requires_approval=True` is a `Tool(...)` / `@agent.tool` kwarg; the run output type must
  include `DeferredToolRequests` (`output_type=[str, DeferredToolRequests]`).
- Model instances can't cross the activity boundary, so the agent's model is a `RuntimeModel`
  that resolves the real model (LiteLLM, or `TestModel` in tests) from `toy.runtime` inside the
  activity. The same holder provides the Redis publisher to the event handler.
- `asyncio.wait` is restricted in the workflow sandbox; the turn uses `workflow.wait`.
- The time-skipping test server can't be downloaded in this environment, so workflow tests
  run against the compose Temporal server with a unique task queue per test.

### Step 6: Vercel AI SDK UI message stream at the SSE edge

`toy/api/vercel_encoder.py` is the default encoder for `GET /agent/stream`: a stateful
per-connection translator from `StreamEvent` to AI SDK UI message stream chunks (the chunk
models come from `pydantic_ai.ui.vercel_ai.response_types`, so the wire format follows the SDK).
Framing is `id: <redis stream id>\ndata: <json>\n\n`, the response carries
`x-vercel-ai-ui-message-stream: v1`, and `data: [DONE]` follows every `turn.completed`.

| Normalized event | Chunk(s) |
| --- | --- |
| `turn.started` | `start{messageId=turn id}`, `start-step` |
| `text.*`, `reasoning.*` | `text-start/delta/end`, `reasoning-start/delta/end` (ids are `turn:attempt:part`) |
| `tool.input_start/delta/available`, `tool.output` | `tool-input-start/delta/available`, `tool-output-available` or `tool-output-error` |
| `status.changed` | `data-status`, `data-queue` |
| `request.resolved` | `data-request-resolved` |
| attempt bump (retried model activity) | closes open parts, `data-retry`, new part ids |
| `turn.completed` | open parts closed, `error` (failed), `data-approval` (blocked), `finish-step`, `finish{finishReason}` then `[DONE]` |

Finish reasons: completed→`stop`, stopped/blocked→`other`, failed→`error`.
`?format=raw` keeps the JSON debug encoder. Tests: `tests/test_vercel_encoder.py` (synthetic
fixtures plus `tests/fixtures/hitl_recorded.json`, recorded from the step 5 live run).

### Step 7: usage-based billing

`toy/billing.py`: `BillingModel(WrapperModel)` wraps the real model *inside the model activity*
(the worker installs it as `runtime.model`). After each request (streamed or not) it computes
usage and cost from a small per-token price table and posts one meter event
`{event_name, identifier, customer, value, model, usage, cost_usd, partial, attempt}` to
`settings.billing_url`. `identifier = uuid5(NS, "{workflow_id}:{run_id}:{activity_id}")`, so a
retried activity re-posts the same identifier and the endpoint answers 409 (counted once). A
request that raises after producing usage is a partial completion: it gets `:attempt{n}` and is
metered on its own. Successful metering also publishes a `usage.updated` stream event.

Vercel's `emulate` Stripe emulator covers customers, payments and checkout but not billing meter
events, so `toy/billing_emulator.py` (FastAPI, port 8402) stands in: `POST
/v1/billing/meter_events` (409 on duplicate identifier), `GET` to list, `DELETE` to reset.

```bash
uv run python -m toy.billing_emulator      # port 8402 (BILLING_PORT)
uv run pytest tests/test_billing.py        # identity, retry dedup, partial, emulator 409
uv run pytest tests/test_workflow.py -k retry   # forced model-activity retry: partial + full
curl -s localhost:8402/v1/billing/meter_events | jq .total_value
```

Note on the retry test: the forced failure happens *inside* the model stream (the event handler
raises), so attempt 1 is metered as a partial event and attempt 2 as the full one; the same
completion is never counted twice (`test_retried_call_meters_once`).

### Step 8: frontend

`frontend/` (Next.js, pnpm, TypeScript) uses the Vercel AI SDK `useChat` with a custom
`ChatTransport` (`frontend/lib/transport.ts`): `sendMessages` POSTs a `send` command and the
reply arrives on the conversation's SSE stream opened by `reconnectToStream` from the last SSE
id. The SDK's resume path resets per `start` chunk, so every turn is its own assistant message
and one stream serves the whole conversation. Queue panel (edit, delete, reorder, send now),
steer bar, stop button (`expected_turn_id`) and approval card POST commands directly and read
`GET /agent/conversations/{id}/state`. See `frontend/README.md` for details and the Playwright
smoke (`e2e/smoke.spec.ts`, run against the live stack).

Changes made on the backend for the UI: `data-status` / `data-queue` / `data-retry` are
transient data parts; a blocked turn emits `tool-approval-request` chunks before `data-approval`
and the resuming turn re-announces the approved call (`tool-input-available` with the recorded
args) before its output, so the AI SDK can attach the output to a tool part in the new message;
`GET /agent/stream?once=true` closes after the next `turn.completed` (single-turn clients and
the API test); the SSE loop consumes the Redis tail in a pump task so keepalive timeouts never
cancel the blocking read.

Mismatches: the `ai-elements` and `shadcn` registries were unreachable from this environment, so
`frontend/components/ai-elements/*` are hand-written stand-ins with the AI Elements names and
props. redis-py 8 applies a 5 s default socket timeout that killed `XREAD BLOCK 5000`; the
client is now created with a 30 s socket timeout and the tail tolerates read timeouts.

### Step 9: human-in-the-loop validation

`scripts/validate_hitl.py` drives the API end to end (stack up, `LLM_MODEL=test` worker,
`agent-api`): for `accept` and then `decline`, each in its own conversation, it sends a prompt
that triggers `create_ticket`, asserts `status == blocked` with exactly one pending approval,
posts `tool_results`, waits for the resuming turn to complete, then checks Postgres (turn-1
rows `blocked`, turn-2 rows `completed`, the `create_ticket` tool-return content, the status
mirror) and the Redis event sequence (`tool.input_available … turn.completed{blocked} …
request.resolved … turn.completed{completed}`, the approved flag, the tool output or the denial
reason).

```bash
uv run python scripts/validate_hitl.py       # prints ok/FAIL per assertion; exit 1 on failure
```

One conversation per decision because Pydantic AI's `TestModel` stops calling tools once the
history contains tool returns, so a second round in the same conversation would not block.

### Step 10: claim check

`src/toy/claim_check.py`: `ClaimCheckCodec(PayloadCodec)` gzips any payload over 20 KB, stores
it in S3 under its sha256 (`put_if_absent`, so retries and identical histories dedupe) and
leaves a `binary/claim-check` claim in the history; a missing object decodes to a non-retryable
`ApplicationError`. `ClaimCheckPlugin(SimplePlugin)` replaces only `payload_codec` on the
existing data converter, so Pydantic AI's payload converter stays. Both the API client and the
worker install it when `CLAIM_CHECK_ENABLED=true` (default; `CLAIM_CHECK_THRESHOLD` bytes).

Compose gains `minio` (API 9000, console 9001, bucket `claim-check` created on first use).
Docker Hub denied `minio/minio` from this environment, so `docker-compose.s3mock.yml` swaps in
`adobe/s3mock` on the same port for validation:

```bash
docker compose -f docker-compose.yml -f docker-compose.s3mock.yml up -d minio   # or plain `up -d` with MinIO
uv run pytest tests/test_claim_check.py          # codec round trip, dedup, missing object, plugin through Temporal
uv run python scripts/measure_claim_check.py     # 50-turn history with and without the codec
```

Validated live: a 46 KB `send` produced three `binary/claim-check` payloads in the workflow
history and three objects in the bucket, and the turn ran normally. Measurement and the
recommendation are in `DECISION.md` (short version: required for the product; 50 turns of 2 KB
replies already write 5 MB of history without it, and ~53 history events per turn mean
continue-as-new is needed regardless).
