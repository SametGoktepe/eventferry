# @eventferry/kafka

## 1.0.2

### Patch Changes

- 89f1867: Declare `engines.node` (>=18) so npm shows the supported Node version and tooling can warn on unsupported runtimes.
- Updated dependencies [89f1867]
  - @eventferry/core@1.0.2

## 1.0.1

### Patch Changes

- docs: polish per-package READMEs (npm page content). No code changes.
- Updated dependencies
  - @eventferry/core@1.0.1

## 1.0.0

### Minor Changes

- b06f8ec: Strict per-aggregate ordering, crash recovery, and driver/packaging fixes.

  - **postgres:** the claim query now enforces strict per-aggregate ordering by
    only taking the _head_ of each aggregate (no earlier unfinished row for the
    same `aggregateId`). At most one in-flight message per aggregate; failed
    messages block their successors until resolved.
  - **postgres:** added a `claimed_at` column and a visibility-timeout reaper
    (`claimTimeoutMs`, default 60s) so rows orphaned by a crashed relay are
    reclaimed instead of stuck in `processing` forever. Migration is upgrade-safe
    (`ADD COLUMN IF NOT EXISTS`); the partial indexes were retuned for the new
    ordered, reaper-aware claim.
  - **core:** dead-lettered messages now carry the real `original-topic` header
    (previously always empty); `ConsoleLogger` routes warn/error to the matching
    `console` methods.
  - **kafka:** the confluent driver now honors `acks` and `compression` (it
    silently ignored them before), matching the kafkajs driver.
  - **packaging:** the `@eventferry/postgres/migrations` subpath export now
    advertises its types; `pnpm-workspace.yaml` dropped an invalid placeholder
    block.

  Note: `claimTimeoutMs` should exceed your worst-case publish latency. This is
  an at-least-once system — pair it with idempotent producers/consumers.

### Patch Changes

- b06f8ec: Fix the kafkajs driver using `producer.send` with a multi-topic `topicMessages`
  payload, which kafkajs rejects with "Invalid topic" — the `topicMessages` form is
  `producer.sendBatch`. Batches now publish correctly (caught by the new integration
  suite against real Redpanda; unit tests used a fake producer that didn't validate).
- Updated dependencies [b06f8ec]
- Updated dependencies [b06f8ec]
- Updated dependencies [b06f8ec]
- Updated dependencies [b06f8ec]
- Updated dependencies [b06f8ec]
  - @eventferry/core@1.0.0
