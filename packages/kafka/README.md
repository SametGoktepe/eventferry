# @eventferry/kafka

[![npm](https://img.shields.io/npm/v/@eventferry/kafka.svg)](https://www.npmjs.com/package/@eventferry/kafka)

The **Kafka / Redpanda publisher** for [eventferry](https://github.com/SametGoktepe/eventferry) —
a transactional outbox toolkit for PostgreSQL + Kafka/Redpanda.

One `KafkaPublisher` over two interchangeable drivers — `kafkajs` (pure JS) and
`@confluentinc/kafka-javascript` (librdkafka-backed) — with idempotent and optional
transactional (EOS) producers, plus dead-letter (DLQ) routing.

## Install

```bash
npm i @eventferry/kafka @eventferry/core

# pick ONE Kafka client (both are optional peers):
npm i kafkajs                          # pure JS, zero native deps
npm i @confluentinc/kafka-javascript   # librdkafka-backed, higher throughput
```

## Usage

```ts
import { KafkaPublisher } from "@eventferry/kafka";

const publisher = new KafkaPublisher({
  driver: "kafkajs", // or "confluent"
  brokers: ["localhost:19092"],
  idempotent: true,
});

// Hand it to a Relay from @eventferry/core.
```

You can also pass a `customDriver` implementing the `KafkaDriver` interface.

## Authentication & TLS

### One-way TLS

```ts
new KafkaPublisher({
  brokers: ["broker:9093"],
  ssl: true, // uses the driver's default trust store
});
```

### mTLS (mutual TLS)

```ts
import { readFileSync } from "node:fs";

new KafkaPublisher({
  brokers: ["broker:9093"],
  ssl: {
    ca: readFileSync("/etc/ssl/kafka-ca.pem"),
    cert: readFileSync("/etc/ssl/client.pem"),
    key: readFileSync("/etc/ssl/client-key.pem"),
    passphrase: "optional",
    // servername: "broker.example.com",   // SNI override if cert SAN differs
  },
});
```

> `rejectUnauthorized` is intentionally NOT a knob. TLS verification is
> non-negotiable. For dev clusters with self-signed certs, pass the cluster
> CA via `ca` so verification succeeds.

### SASL — username + password (PLAIN / SCRAM)

```ts
new KafkaPublisher({
  brokers: ["broker:9093"],
  ssl: true,
  sasl: {
    mechanism: "scram-sha-512", // or "plain" | "scram-sha-256"
    username: process.env.KAFKA_USER!,
    password: process.env.KAFKA_PASSWORD!,
  },
});
```

### SASL/OAUTHBEARER (Azure Event Hubs, OIDC, MSK IAM)

```ts
new KafkaPublisher({
  brokers: ["broker:9093"],
  ssl: true,
  sasl: {
    mechanism: "oauthbearer",
    oauthBearerProvider: async () => {
      const token = await myTokenIssuer();
      return {
        value: token.value,           // required for both drivers
        principal: token.principal,   // required for confluent driver
        lifetime: token.expiresInMs,  // required for confluent driver
        extensions: token.extensions, // optional
      };
    },
  },
});
```

> **Driver asymmetry:** `kafkajs` reads only `value`; `@confluentinc/kafka-javascript` requires `value` + `principal` + `lifetime` (in milliseconds) and accepts an optional `extensions` map. Cross-driver portable providers should populate all four fields.

## Producer tuning

The high-throughput recipe (confluent driver):

```ts
new KafkaPublisher({
  driver: "confluent",
  brokers: ["broker:9092"],
  idempotent: true,
  compression: "zstd",
  lingerMs: 25,        // batch up to 25ms for higher throughput
  batchSize: 131_072,  // 128 KB per partition batch
  maxInFlightRequests: 5,
  maxRequestSize: 2_000_000,
});
```

Driver support matrix:

| Knob | `kafkajs` | `confluent` |
|---|:--:|:--:|
| `transactionTimeoutMs` | ✅ | ✅ |
| `requestTimeoutMs` | ✅ | ✅ |
| `maxInFlightRequests` | ✅ | ✅ |
| `lingerMs` | ⚠️ warn + ignore | ✅ |
| `batchSize` | ⚠️ warn + ignore | ✅ |
| `deliveryTimeoutMs` | ⚠️ warn + ignore | ✅ |
| `maxRequestSize` | ⚠️ warn + ignore | ✅ |

`kafkajs` has no equivalent producer-level config for the last four — its batching is sticky-partitioner + hardcoded internals. The typed API accepts them for portability; on the kafkajs driver they log a one-time warning and are otherwise ignored. Use the confluent driver when you need fine-grained tuning.

## Partitioning

### Default (key-based, java-compatible)

By default a record's `key` is hashed (murmur2, matching the Java client) and the partition derived from it. Same key → same partition → ordered stream per aggregate. No config needed.

### Explicit partition override

Pin a record to a specific partition by setting `partition` on the
`PublishableMessage` — for compacted topics with application-managed sharding, tenant-affinity routing, or geo-pinning:

```ts
const msg: PublishableMessage = {
  topic: "orders.created",
  key: "tenant-a:order-42",
  value: encoded,
  headers: {},
  recordId: row.id,
  messageId: row.message_id,
  partition: 3, // ← pins this record to partition 3
};
```

### kafkajs partitioner choice

The kafkajs driver exposes the v2 partitioner selection (and silences the
`KafkaJSPartitionerNotSpecified` warning):

```ts
new KafkaPublisher({
  driver: "kafkajs",
  brokers: ["broker:9092"],
  partitioner: "java-compatible", // (default) | "legacy" | "default"
});
```

- `"java-compatible"` — kafkajs's `JavaCompatiblePartitioner`; greenfield recommendation, matches the Java client's murmur2.
- `"legacy"` — pre-v2 hashing. Use when migrating an existing topic to keep hash continuity.
- `"default"` — kafkajs's current default. May change in future major versions.

📖 **Full documentation:** [github.com/SametGoktepe/eventferry](https://github.com/SametGoktepe/eventferry#readme)

## License

MIT
