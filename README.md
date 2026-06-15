<h1 align="center">Eventferry</h1>
<p align="center">
  Transactional outbox for <b>PostgreSQL + Kafka/Redpanda</b> — reliable event publishing for Node.js &amp; TypeScript.<br>
  Write your data and your events in <b>one transaction</b>; eventferry ferries them to the broker — at&#8209;least&#8209;once, in order, with retries and dead&#8209;lettering.
</p>

<p align="center">
  <a href="https://github.com/SametGoktepe/eventferry/actions/workflows/ci.yml"><img src="https://github.com/SametGoktepe/eventferry/actions/workflows/ci.yml/badge.svg" alt="CI status"></a>
  <a href="https://www.npmjs.com/package/@eventferry/core"><img src="https://img.shields.io/npm/v/@eventferry/core.svg" alt="npm version"></a>
  <a href="https://www.npmjs.com/package/@eventferry/core"><img src="https://img.shields.io/npm/dm/@eventferry/core.svg" alt="npm downloads"></a>
  <img src="https://img.shields.io/badge/core%20deps-0-brightgreen.svg" alt="zero core dependencies">
  <img src="https://img.shields.io/badge/types-included-blue.svg" alt="types included">
  <a href="./LICENSE"><img src="https://img.shields.io/npm/l/@eventferry/core.svg" alt="MIT license"></a>
  <img src="https://img.shields.io/badge/node-%3E%3D18-brightgreen.svg" alt="node >=18">
</p>

```
┌─────────────┐   one TX     ┌──────────────┐   relay     ┌───────────────┐
│  your code  │ ───────────▶ │  outbox tbl  │ ──────────▶ │ Kafka/Redpanda│
│ (order svc) │   (atomic)   │  (Postgres)  │   publish   │     topic     │
└─────────────┘              └──────────────┘             └───────────────┘
```

eventferry implements the **transactional outbox** pattern so you don't have to
hand-roll it. Instead of publishing to Kafka directly (which can lose events on a crash,
or publish events for data that rolled back), you write the event into an outbox table in
the **same database transaction** as your data. A background **relay** then reliably ships
those rows to the broker. The database is the source of truth; nothing is lost or invented.

## Features

* ✅ **Atomic** — your data and its event commit together, or not at all. No lost or phantom events.
* 🔁 **At-least-once delivery** with idempotent producers (pair with idempotent consumers).
* 🔢 **Strict per-aggregate ordering** — events for the same entity arrive in order, even across many relays and retries.
* ♻️ **Retries** with fixed / linear / exponential backoff + jitter, and **dead-letter** routing for poison messages.
* 🛟 **Crash recovery** — work orphaned by a dead relay is reclaimed automatically (visibility timeout).
* ⚡ **Horizontal scale** — run any number of relays against one table, lock-free (`FOR UPDATE SKIP LOCKED`).
* 🧷 **Type-safe, schema-validated events** (optional) — a bad payload can't reach the outbox. Any [Standard Schema](https://standardschema.dev) (Zod, Valibot, ArkType…).
* 🚀 **Low-latency modes** — `LISTEN/NOTIFY` wake-ups, or WAL streaming (logical replication) when polling isn't fast enough.
* 🧰 **Two Kafka clients** — `kafkajs` and `@confluentinc/kafka-javascript` behind one API.
* 📦 **Schema Registry** — Avro / Protobuf / JSON Schema via Confluent Schema Registry.
* 🔭 **Observability** — metrics hooks + W3C / OpenTelemetry trace propagation.
* 🪶 **Zero runtime dependencies** in the core; everything else is an optional peer.

## Quick start

```bash
npm i @eventferry/core @eventferry/postgres @eventferry/kafka pg kafkajs
```

```ts
import { Relay } from "@eventferry/core";
import { PostgresStore, createMigrationSql } from "@eventferry/postgres";
import { KafkaPublisher } from "@eventferry/kafka";

// 1. Create the outbox table (idempotent — safe to run on boot).
await pool.query(createMigrationSql("outbox"));

const store = new PostgresStore({ pool });

// 2. Write side: enqueue the event in the SAME transaction as your data.
const client = await pool.connect();
try {
  await client.query("BEGIN");
  await client.query("INSERT INTO orders (...) VALUES (...)");
  await store.enqueue(client, {
    topic: "orders.created",
    aggregateType: "order",
    aggregateId: order.id, // → Kafka partition key (preserves per-entity order)
    payload: { orderId: order.id, total: order.total },
  });
  await client.query("COMMIT"); // order + event commit atomically
} catch (e) {
  await client.query("ROLLBACK");
  throw e;
} finally {
  client.release();
}

// 3. Publish side (a separate worker): drain the outbox to Kafka.
const relay = new Relay({
  store,
  publisher: new KafkaPublisher({ brokers: ["localhost:19092"], idempotent: true }),
  retry: { maxAttempts: 5, strategy: "exponential", baseMs: 200, maxMs: 30_000 },
  dlq: { topic: "orders.dlq" },
});
await relay.start();
process.on("SIGTERM", () => relay.stop());
```

That's the whole pattern. Everything below is optional power.

## Why eventferry?

Most outbox libraries lock you to one Kafka client, only poll, and skip ordering,
idempotency, or crash recovery. eventferry is a complete, production-grade toolkit:

| Concern | eventferry | Typical outbox lib |
| --- | --- | --- |
| Kafka client | **kafkajs *and* confluent** behind one API | one, hard-coded |
| Delivery | idempotent + optional transactional (EOS) | best-effort |
| Ordering | **strict per-aggregate**, across relays & retries | none / global only |
| Retries & DLQ | backoff + jitter → dead-letter topic | basic / none |
| Crash recovery | visibility-timeout reaper | rows can get stuck |
| Latency | poll **+ LISTEN/NOTIFY + WAL streaming** | poll only |
| Type safety | typed + schema-validated payloads | `any` |
| Serialization | JSON + Schema Registry (Avro/Proto/JSON) | JSON |
| Tracing | W3C / OpenTelemetry propagation | none |
| Footprint | zero-dep core; storage/broker pluggable | varies |

## What eventferry is *not*

* **Not a message broker or queue** — it reliably bridges your DB to Kafka/Redpanda; the broker does delivery.
* **Not a consumer framework** — it *publishes* events. Consuming them is your job (a typed `decode` helper is provided, but no subscription loop).
* **Not an ORM or migration tool** — you bring your own `pg` pool and migration runner; eventferry hands you the SQL to run.
* **Not exactly-once end-to-end** — at-least-once by default (with an optional EOS producer for the broker hop). Make consumers idempotent.

## Installation

eventferry is a small set of focused packages. **For everything in one shot**, install
the meta-package and your Kafka client:

```bash
npm i @eventferry/all pg kafkajs   # pulls in core + postgres + kafka + schema-registry
```

Or install only the pieces you use (smaller dependency tree) — the core, a storage
adapter, a broker adapter, and your chosen Kafka client:

```bash
# core engine + Postgres store + Kafka publisher + the pg driver
npm i @eventferry/core @eventferry/postgres @eventferry/kafka pg

# pick ONE Kafka client (both are optional peers):
npm i kafkajs                          # pure JS, zero native deps
npm i @confluentinc/kafka-javascript   # librdkafka-backed, higher throughput

# optional add-ons:
npm i @eventferry/schema-registry @kafkajs/confluent-schema-registry  # Avro/Proto/JSON
npm i pg-logical-replication                                          # WAL streaming mode
```

> **Note**
> Requires Node.js 18+, PostgreSQL 13+, and Kafka or Redpanda. The streaming relay also
> needs `wal_level = logical` on the server.

## Usage

### Create the outbox table

`createMigrationSql` returns idempotent DDL (table + indexes). Run it however you manage
schema — raw, Flyway, Prisma, node-pg-migrate, etc.

```ts
import { createMigrationSql } from "@eventferry/postgres/migrations";
await pool.query(createMigrationSql("outbox"));
```

### Type-safe events (`defineOutbox`)

Declare topics once — aggregate type + payload schema — and get a typed, validated
`enqueue` plus a `decode` consumers can reuse. The schema is any Standard Schema; there is
**no validator dependency** in the package.

```ts
import { z } from "zod";
import { defineOutbox } from "@eventferry/core";

const registry = {
  "orders.created": { aggregateType: "order", schema: z.object({ orderId: z.string(), total: z.number() }) },
  "orders.shipped": { aggregateType: "order", schema: z.object({ orderId: z.string(), carrier: z.string() }) },
} as const;

const outbox = defineOutbox(registry, { store });

// Typed + validated before the row is inserted; a bad payload throws and your TX rolls back.
await outbox.enqueue(client, "orders.created", {
  aggregateId: order.id,
  payload: { orderId: order.id, total: order.total }, // ✓ typed from the schema
});

// In a consumer (same registry, no store): decode back to the typed, validated payload.
const event = await defineOutbox(registry).decode("orders.created", message.value);
//    ^? { orderId: string; total: number }
```

### Ordering & crash recovery

The claim query only takes a row when it is the **head** of its aggregate (no earlier
unfinished row for the same `aggregateId`). So at most one message per aggregate is
in-flight, a failed message blocks its successors until retried, and the broker never
sees same-key messages out of order — even across concurrent relays.

Each claim stamps `claimed_at`; a row left in `processing` longer than `claimTimeoutMs`
is reclaimed (its relay is presumed dead):

```ts
new PostgresStore({ pool, claimTimeoutMs: 60_000 }); // default 60s
```

> **Warning**
> Set `claimTimeoutMs` above your worst-case publish latency. If it fires while a slow-
> but-alive relay is still in flight, the row is republished — a duplicate that idempotent
> consumers absorb (this is an at-least-once system).

### Scaling: run many relays

`claimBatch` uses `SELECT ... FOR UPDATE SKIP LOCKED`, so any number of relay instances
can run against the same table — each claims a disjoint set of rows, none block on the
others, and per-aggregate ordering still holds.

### Exactly-once to the broker (transactional producer)

```ts
new KafkaPublisher({
  driver: "confluent",
  brokers: ["localhost:19092"],
  idempotent: true,
  transactional: true,
  transactionalId: "eventferry-relay-1", // stable per relay instance
});
```

### Kafka drivers

`kafkajs` and `@confluentinc/kafka-javascript` are interchangeable behind one
`KafkaPublisher` — same options, same behavior. Pass a `customDriver` for testing or
unsupported clients.

```ts
new KafkaPublisher({ driver: "kafkajs", brokers });   // easy to deploy
new KafkaPublisher({ driver: "confluent", brokers });  // higher throughput
```

### Low latency: notify & streaming

Polling adds up to one `pollIntervalMs` of delay. Two faster modes:

**Notify-driven (`LISTEN/NOTIFY`)** — claims the instant a row commits; polling stays on
as a safety net. No extra dependency.

```ts
import { createNotifyTriggerSql } from "@eventferry/postgres/migrations";
import { PostgresNotifyWaker } from "@eventferry/postgres";
import { Client } from "pg";

await pool.query(createNotifyTriggerSql("outbox", "outbox")); // one-time trigger

new Relay({
  store, publisher,
  pollIntervalMs: 5_000, // now just a safety net
  waker: new PostgresNotifyWaker({ connect: () => new Client(connStr), channel: "outbox" }),
});
```

**Streaming (logical replication)** — publishes straight from the WAL with
`PostgresStreamingRelay`, with no claim query on the happy path (lower DB load). Failures
fall back to the claim-based retry/DLQ loop, so those guarantees are reused.

```ts
import { createPublicationSql } from "@eventferry/postgres/migrations";
import { PostgresStreamingRelay } from "@eventferry/postgres";

await pool.query(createPublicationSql("outbox", "outbox_pub"));

await new PostgresStreamingRelay({
  store: new PostgresStore({ pool, claimFailedOnly: true }),
  publisher,
  replication: { connectionString: connStr, slot: "outbox_slot", publication: "outbox_pub", table: "outbox" },
  dlq: { topic: "orders.dlq" },
}).start();
```

> **Note**
> Streaming is **best-effort** per-aggregate ordering (a retried failure can land after
> later same-aggregate rows). Use the polling relay if you need the strict guarantee.

### Schema Registry (Avro / Protobuf / JSON Schema)

Encode payloads in the Confluent wire format instead of JSON — a drop-in `serializer`:

```ts
import { SchemaRegistrySerializer } from "@eventferry/schema-registry";

const serializer = new SchemaRegistrySerializer({
  host: "http://localhost:8081",
  schemas: { "orders.created": { type: "AVRO", schema: orderCreatedAvsc } },
});

new Relay({ store, publisher, serializer }); // also works with PostgresStreamingRelay
```

### Tracing (W3C / OpenTelemetry)

A `traceparent` captured at enqueue rides to the published message on every path, so the
consumer continues the trace. The library depends on **no** tracing package:

```ts
import { propagation, context } from "@opentelemetry/api";

new PostgresStore({
  pool,
  tracing: { inject: (carrier) => propagation.inject(context.active(), carrier) },
});
```

### Observability

```ts
new Relay({
  hooks: {
    onBatchClaimed: (n) => metrics.gauge("outbox.batch", n),
    onPublished: (r) => metrics.increment("outbox.published"),
    onDead: (rec, err) => alert(`dead message ${rec.id}: ${err.message}`),
  },
});
```

Each published message carries `message-id`, `aggregate-type`, `aggregate-id`,
`content-type`, and (when present) `trace-id` headers.

### Retention

Published rows accumulate as `done`. Indexes exclude them so queries stay fast, but the
table grows; `purgeDone` batch-deletes old ones (run from your own cron):

```ts
await store.purgeDone({ olderThanMs: 7 * 24 * 60 * 60 * 1000 }); // older than 7 days
```

## Packages

| Package | What it is |
| --- | --- |
| [`@eventferry/core`](./packages/core) | DB- and broker-agnostic engine: relay loop, backoff, serializer, typed registry. Zero runtime deps. |
| [`@eventferry/postgres`](./packages/postgres) | PostgreSQL store, migration/trigger/publication SQL, notify waker, streaming relay, retention. |
| [`@eventferry/kafka`](./packages/kafka) | Kafka/Redpanda publisher over `kafkajs` and `confluent` drivers, with DLQ routing. |
| [`@eventferry/schema-registry`](./packages/schema-registry) | Confluent Schema Registry serializer (Avro / Protobuf / JSON Schema). |
| [`@eventferry/all`](./packages/all) | Meta-package — installs & re-exports all of the above. `npm i @eventferry/all` for everything in one import. |

## Roadmap

eventferry is built around a small, database-agnostic `OutboxStore` contract, so
every new database is just a new adapter. PostgreSQL ships today; **MySQL/MariaDB,
SQL Server, and MongoDB are next**, with CockroachDB, SQLite, Oracle, and DynamoDB
on the horizon.

See **[ROADMAP.md](./ROADMAP.md)** for the full plan — architecture diagrams, the
per-database capability matrix, and phase-by-phase checklists.

## Development

```bash
pnpm install
pnpm build
pnpm typecheck
pnpm test:run          # unit tests (fakes, no infra)
pnpm test:integration  # real Postgres + Redpanda via Testcontainers (needs Docker)
```

## Contributing

Issues and pull requests are welcome. Run `pnpm test:run` and `pnpm typecheck` before
opening a PR; add a [changeset](https://github.com/changesets/changesets) for any
user-facing change.

## License

[MIT](./LICENSE) © Samet GOKTEPE
