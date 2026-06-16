---
"@eventferry/mysql": minor
---

feat: new `@eventferry/mysql` adapter — MySQL **8.0.1+** / MariaDB **10.6+** outbox store with **CDC binlog streaming** mode at parity with the Postgres adapter.

- `MysqlStore.claimBatch` uses `SELECT ... FOR UPDATE SKIP LOCKED` inside an internal transaction (MySQL has no `RETURNING`), preserves strict per-aggregate ordering via the same NOT-EXISTS head guard as the Postgres adapter, and is reaper-aware.
- `createMigrationSql` ships the InnoDB + utf8mb4 + DATETIME(3) + JSON schema.
- `MysqlBinlogRelay` tails the MySQL binary log (row-based) via the optional `@vlasky/zongji` peer dep — the same mechanism Debezium uses. Bypasses the polling claim loop for low-latency / high-throughput workloads, with the failed-row internal retry loop reusing the engine's backoff / DLQ / dead handling.

No native low-latency waker (MySQL has no `LISTEN/NOTIFY`); use the binlog relay or tune the poll interval down.
