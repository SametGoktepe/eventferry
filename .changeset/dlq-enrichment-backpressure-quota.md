---
"@eventferry/core": minor
"@eventferry/postgres": minor
"@eventferry/mysql": minor
"@eventferry/kafka": minor
---

**feat: DLQ enrichment + backpressure runtime + quota multiplier — Tier 1 of the reliability gap closed**

### DLQ enrichment

Records routed to the dead-letter queue now carry the full context an operator needs to triage:

| Header | Set by | Note |
|---|---|---|
| `original-topic` | relay | already existed |
| `dlq-reason` | publisher | already existed (`error.message`) |
| `dlq-failed-at` | publisher | already existed (ISO timestamp) |
| `dlq-error-class` | publisher | **new** — `error.name` / constructor name |
| `dlq-attempts` | relay | **new** — string-encoded `attempts` count |
| `dlq-original-aggregate-id` | relay | **new** — for joining with business state |
| `dlq-original-message-id` | relay | **new** — for dedup / idempotency lookups |
| `dlq-error-stack` | relay | **new** — opt-in via `DlqConfig.includeStackTraces`, truncated to `maxStackBytes` (default 4 KB) |

```ts
new Relay({
  store, publisher,
  dlq: { topic: "orders.dlq", includeStackTraces: true, maxStackBytes: 4096 },
});
```

### Backpressure runtime behavior

When the driver classifies a failure as `errorKind: "backpressure"` (client-side producer queue full), the relay no longer treats it like a regular retriable failure. Instead:

- The record is re-queued via the new `OutboxStore.requeue(id, retryAt)` method,
- `attempts` is **not incremented** — the buffer being full is a "slow down" signal, not the record's fault,
- The retry is scheduled `RetryConfig.backpressureDelayMs` ms ahead (default 1000 ms).

Stores that don't implement `requeue` fall back to `markFailed` (with attempts++); both `@eventferry/postgres` and `@eventferry/mysql` ship a real implementation.

### Quota multiplier

When the driver classifies a failure as `errorKind: "quota"` (broker `THROTTLING_QUOTA_EXCEEDED`), the scheduled retry delay is multiplied by `RetryConfig.quotaMultiplier` (default 5) so the producer gives the broker breathing room. Quota failures DO count as attempts — after the budget is exhausted the record routes to DLQ + `dead`.

### New / changed types

- `RetryConfig` gains `backpressureDelayMs?` and `quotaMultiplier?`.
- `DlqConfig` gains `includeStackTraces?` and `maxStackBytes?`.
- `OutboxStore.requeue?(recordId, retryAt)` is a new **optional** method. Stores without it fall through to `markFailed`.

### Backward compatibility

Pure-additive everywhere. Default behavior matches the prior release:

- A `RetryConfig` without `backpressureDelayMs` uses 1000 ms (sensible default).
- A `DlqConfig` without `includeStackTraces` keeps DLQ messages small (default off).
- An `OutboxStore` without `requeue` falls back to `markFailed` — same as before, just with a documented quirk.

This closes the last three Tier 1 items in `docs/kafka-gap-analysis/reliability.md`. Phase A reliability surface is now ~100% complete.
