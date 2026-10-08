# Claim check for Temporal payloads: when it is required

## The limits

Temporal (compose server, `docker/temporal/development-sql.yaml`, which pins the defaults):

| Limit | Value | What it caps |
| --- | --- | --- |
| `limit.blobSize.error` | 2 MB | one payload: an activity input or result, a workflow input/result, a signal/update argument |
| `limit.historySize.error` | 50 MB | the event history of one workflow *run* |
| `limit.historyCount.error` | 51 200 events | the event history of one workflow run |

`TemporalDurability` runs every model request as an activity whose input carries the **full
`message_history`** of the run (plus the serialized run context), and whose result carries the
`ModelResponse`. Every tool call is another activity with its own input and result. So each turn
writes the whole conversation into history at least once per model call, and the per-call
payload grows linearly with the conversation.

## Measurement (`scripts/measure_claim_check.py`, results in `scripts/claim_check_measurement.json`)

50 turns of `ConversationWorkflow` against the compose server, a model that answers with ~2 KB
of text per turn, no tool calls, one run (no continue-as-new):

| | without codec | with claim check (20 KB threshold) |
| --- | --- | --- |
| history events | 2 633 | 2 639 |
| history bytes | 5 046 718 | 1 004 068 (20 %) |
| largest payload | 161 119 B | 20 015 B |
| payloads ≥ 20 KB | 44 | 0 |
| objects stored in S3 | – | 44 (4.06 MB raw, 154 KB gzip; lorem text compresses ~26×, real chat text ~3–5×) |

What the numbers say:

- **Events per turn ≈ 53** (model activity, our `publish_event` / `mirror_status` /
  `persist_turn` activities, per-event handler activities, each ≈5 history events). The
  51 200-event cap is reached after **≈ 950 turns** in one run regardless of payload size, so a
  continue-as-new policy is needed anyway; `TURNS_PER_RUN = 20` is conservative by ~50×, but
  each continue-as-new carries only `pending` + counters (history is reloaded from Postgres), so
  it is cheap.
- **Per-run history bytes grow quadratically** with the turn count: each model call re-sends the
  whole history. Without the codec 50 turns of 2 KB replies already cost 5 MB; at ~2.5 KB per
  turn of accumulated messages, the 2 MB *per-payload* limit is hit around turn **800** and the
  50 MB *per-run* limit around turn **160** (sum of 1..n × 2.5 KB ≈ 50 MB → n ≈ 200, minus
  overhead). With long tool outputs (a 100 KB document read) a single model call carries that
  100 KB on every subsequent call: 20 turns later the run is at ~2 MB of payload per call and
  ~20 MB of history.
- With the codec, history stays flat (~20 KB per model call: the claim plus the run context and
  small results); the largest payload is bounded by the threshold.

## Decision

The claim check is **required** for the product, not for the toy's test conversations:

1. **Required when** any of these holds, which is the normal case for sf-platform:
   - conversations longer than ~100 turns per workflow run, or continue-as-new less often than
     every ~100 turns;
   - tool results or user content above a few hundred KB (file reads, search results, pasted
     logs): one such payload is re-sent on every model call of every later turn;
   - message history with images/files: Pydantic AI refuses image *output* under Temporal for
     the same 2 MB reason, and binary inputs count against the same cap.
2. **Not needed** for short conversations with small tool outputs, where continue-as-new every
   20 turns keeps a run under ~1 MB of history.

Recommendation: ship `ClaimCheckPlugin` (`src/toy/claim_check.py`) on both the API client and
the worker (`CLAIM_CHECK_ENABLED=true`, threshold 20 KB), content-addressed (sha256 of the
gzipped payload) so retries, replays and identical histories dedupe; keep continue-as-new at
20 turns; store objects with a lifecycle rule matching the namespace's retention. A missing
object is a non-retryable `ApplicationError`: the history is unusable without it, so retention
must outlive the workflow (and its retained history).

## Alternatives considered

- **Temporal's experimental `ExternalStorage` / `StorageDriver`** (temporalio 1.34,
  `DataConverter(external_storage=ExternalStorage(drivers=[...], payload_size_threshold=256 KiB))`).
  Same idea, built into the data converter, with a driver `store()/retrieve()` contract and
  `Payload.external_payloads` claims. Marked experimental and the threshold defaults to 256 KiB;
  revisit once stable, since it would replace our codec with an S3 driver only.
- **Smaller histories instead of larger payloads**: a `ProcessHistory` capability that compacts
  or summarizes old turns before each model request lowers both the model cost and the payload
  size; it is complementary (the codec protects the limits, compaction protects the token bill).
- **Not passing history through the workflow at all**: load it inside the model activity by
  conversation id. That would make the activity non-deterministic with respect to the workflow's
  view of the conversation and is not how `TemporalDurability` works; rejected.
