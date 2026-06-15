---
"@eventferry/postgres": minor
---

Add a retention helper: `PostgresStore.purgeDone({ olderThanMs, batchSize?, maxRows? })`.

Batch-deletes `done` rows whose `processed_at` is older than the cutoff and returns the
total removed, keeping the outbox table from growing unbounded. Run it periodically
(your own scheduler). Only `done` rows are purged.

Also adds `createRetentionIndexSql(table)` — an optional partial index over done rows
(`WHERE status = 2`) that speeds up the purge scan on high-volume tables; the default
indexes intentionally exclude done rows, so add this only if retention scans get slow.
