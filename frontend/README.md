# frontend

Dummy UI for the toy: Vercel AI SDK `useChat` over agent-api, with a queue panel, steer, stop
and an approval card.

```bash
# from the repo root, with docker compose up, the worker and agent-api running:
cd frontend
pnpm install
cp .env.local.example .env.local         # NEXT_PUBLIC_API_URL=http://localhost:8400
pnpm dev                                  # http://localhost:3000/?c=<conversation id>
```

Open http://localhost:3000; without `?c=` a random conversation id is generated and put in the
URL, so reloading reconnects to the same conversation (replaying from the Redis stream).

## How it is wired

- `lib/transport.ts`: `ConversationTransport implements ChatTransport`. `sendMessages` POSTs a
  `send` command (`client_message_id` = the UI message id) and returns an empty stream; the
  reply comes over the conversation's SSE stream instead. `reconnectToStream` opens
  `GET /agent/stream?conversation_id&after=<last SSE id>` and turns SSE frames into
  `UIMessageChunk`s. The AI SDK's resume path (`resumeStream`, `resume: true`) resets its
  message state on every `start` chunk with a new `messageId`, so each turn (one `start … finish
  [DONE]`) becomes its own assistant message and the stream stays open across turns. The SSE
  `id:` is the reconnect cursor.
- `data-status` / `data-queue` / `data-retry` are transient data parts handled in `onData`;
  `data-approval` and `data-request-resolved` stay in the message (the approval card reads the
  latest `data-approval`).
- Queue panel, steer, stop (`expected_turn_id` = the running turn) and approvals POST commands
  directly (`lib/api.ts`) and refresh `GET /agent/conversations/{id}/state` (polled every 2 s and
  on transient data parts).
- `components/ai-elements/*`: small stand-ins for the AI Elements `Conversation`, `Message`,
  `Response` (streamdown), `Tool`, `Reasoning` and `PromptInput` components with the same names
  and props. The `ai-elements` / `shadcn` registries were unreachable from the build
  environment, so they are hand-written; `npx ai-elements@latest add conversation message
  response tool reasoning prompt-input` replaces them where the registry is reachable.

Because every turn is its own assistant message and user messages are appended when sent,
queued messages appear in the transcript before the replies to earlier turns; the queue panel
shows what is still pending.

## Smoke test

```bash
# compose up, LLM_MODEL=test uv run agent-worker, uv run agent-api, pnpm dev
CHROMIUM_PATH=/path/to/chrome pnpm exec playwright test     # e2e/smoke.spec.ts
```

The smoke sends a prompt, waits for `blocked` (TestModel always calls `create_ticket`), approves
through the card and waits for the second assistant message.
