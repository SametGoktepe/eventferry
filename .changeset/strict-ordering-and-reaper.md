---
"@eventferry/core": minor
"@eventferry/postgres": minor
"@eventferry/kafka": minor
---

Strict per-aggregate ordering, crash recovery, and driver/packaging fixes.

- **postgres:** the claim query now enforces strict per-aggregate ordering by
  only taking the *head* of each aggregate (no earlier unfinished row for the
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
