---
"@eventferry/core": minor
"@eventferry/postgres": minor
---

Add a low-latency notify-driven relay (Postgres `LISTEN`/`NOTIFY`).

- **core:** new `Waker` interface and an optional `Relay({ waker })`. The relay's
  idle wait is now interruptible — when the waker signals, it claims immediately
  instead of sleeping out `pollIntervalMs`. With no waker, behavior is unchanged.
- **postgres:** `PostgresNotifyWaker` holds a dedicated `LISTEN` connection and
  wakes the relay on each notification, reconnecting with backoff if it drops.
  `createNotifyTriggerSql(table, channel)` emits an `AFTER INSERT FOR EACH STATEMENT`
  trigger that `pg_notify`s on commit (empty payload — the relay re-claims).
- Polling remains the safety net: a missed notification is caught by the next poll,
  so no event is lost. All ordering/retry/DLQ/crash-recovery guarantees are unchanged.
- No new dependencies (`LISTEN`/`NOTIFY` is native to `pg`).
