# @eventferry/postgres

[![npm](https://img.shields.io/npm/v/@eventferry/postgres.svg)](https://www.npmjs.com/package/@eventferry/postgres)

The **PostgreSQL store** for [eventferry](https://github.com/SametGoktepe/eventferry) —
a transactional outbox toolkit for PostgreSQL / MySQL / MariaDB / MSSQL + Kafka/Redpanda.
For other databases see [`@eventferry/mysql`](https://www.npmjs.com/package/@eventferry/mysql)
or [`@eventferry/mssql`](https://www.npmjs.com/package/@eventferry/mssql).

Provides:

- `PostgresStore` — transaction-joining `enqueue` and a lock-free `claimBatch`
  (`FOR UPDATE SKIP LOCKED`) with **strict per-aggregate ordering** and a
  **crash-recovery reaper** (visibility timeout).
- Migration / trigger / publication SQL generators (`createMigrationSql`,
  `createNotifyTriggerSql`, `createPublicationSql`, `createRetentionIndexSql`).
- `PostgresNotifyWaker` — low-latency `LISTEN`/`NOTIFY` wake-ups.
- `PostgresStreamingRelay` — WAL logical-replication streaming relay.
- `purgeDone` — batched retention of published rows.

## Install

```bash
npm i @eventferry/postgres @eventferry/core pg
```

`pg-logical-replication` is an optional peer, needed only for the streaming relay.

## Usage

```ts
import { PostgresStore, createMigrationSql } from "@eventferry/postgres";

await pool.query(createMigrationSql("outbox")); // idempotent DDL

const store = new PostgresStore({ pool });

// Inside your business transaction:
await store.enqueue(client, {
  topic: "orders.created",
  aggregateType: "order",
  aggregateId: order.id,
  payload: { orderId: order.id, total: order.total },
});
```

📖 **Full documentation:** [github.com/SametGoktepe/eventferry](https://github.com/SametGoktepe/eventferry#readme)

## License

MIT
