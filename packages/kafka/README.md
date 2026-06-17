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

## Transactions (EOS)

### Callable `transactionalId`

`transactionalId` accepts a sync or async resolver — useful when the id depends on runtime context that isn't known at construction time (pod name, AZ + replica index, k8s ordinal):

```ts
new KafkaPublisher({
  brokers,
  transactional: true,
  transactionalId: () =>
    `${process.env.POD_NAME}-${process.env.REPLICA_INDEX}`,
});
```

For multi-instance EOS, the resolved id MUST be stable across a single instance's restarts but UNIQUE across instances. The plain-string form remains supported and unchanged.

### Abort-aware `onTransactionAbort`

When a transactional `sendBatch` triggers the abort path (mid-batch error, broker rejection), the publisher fires `hooks.onTransactionAbort(err)` so dashboards and metrics catch EOS failure rates:

```ts
new KafkaPublisher({
  brokers,
  transactional: true,
  transactionalId: "orders-tx",
  hooks: {
    onTransactionAbort: (err) => metrics.txAborts.inc({ reason: err.name }),
  },
});
```

Best-effort: the hook is safe-wrapped (a throwing hook never breaks the abort path).

## Observability

### Hooks

Wire lifecycle hooks into your metrics / logging stack without subclassing or wrapping the publisher:

```ts
new KafkaPublisher({
  brokers,
  hooks: {
    onConnect:     ()       => readinessProbe.up(),
    onDisconnect:  ()       => readinessProbe.down(),
    onPublish:    (r, msg)  => metrics.publishCounter.inc({ ok: String(r.ok) }),
    onError:      (e, msg)  => sentry.captureException(e, { msg }),
    onTransactionAbort: (e) => metrics.txAborts.inc(),
  },
});
```

Hooks are **safe by construction**: a throwing hook never breaks publishing; the publisher swallows the error and logs it via the configured `logger`.

### OpenTelemetry tracing

The publisher wraps each `publish()` in a span that follows the current stable [OpenTelemetry messaging semantic conventions](https://github.com/open-telemetry/semantic-conventions/blob/main/docs/messaging/kafka.md). No dependency on `@opentelemetry/api` — wire your tracer through a thin adapter:

```ts
import { trace, SpanKind, SpanStatusCode } from "@opentelemetry/api";
import type { KafkaTracer, SpanLike } from "@eventferry/kafka";

const otel = trace.getTracer("@eventferry/kafka");

const tracer: KafkaTracer = {
  startPublishSpan(name, attributes) {
    const span = otel.startSpan(name, { kind: SpanKind.PRODUCER, attributes });
    return {
      setAttribute: (k, v) => span.setAttribute(k, v),
      setAttributes: (a) => span.setAttributes(a),
      setStatus: (s) =>
        span.setStatus({
          code: s.code === "ok" ? SpanStatusCode.OK : SpanStatusCode.ERROR,
          message: s.message,
        }),
      recordException: (e) => span.recordException(e),
      end: () => span.end(),
    } satisfies SpanLike;
  },
};

new KafkaPublisher({ brokers, tracer });
```

Per the spec, eventferry emits **one span per `publish()` call**, named `"{topic} publish"`, with attributes:

| Attribute | Always | Notes |
|---|:--:|---|
| `messaging.system` | ✅ | `"kafka"` |
| `messaging.operation.type` | ✅ | `"publish"` |
| `messaging.destination.name` | ✅ | First topic in the batch |
| `messaging.batch.message_count` | ✅ | Including single-message batches |

The user-supplied tracer SHOULD set `SpanKind.PRODUCER` on the span; the adapter above does this explicitly.

#### Propagating trace context to consumers

Add an optional `inject` method on the tracer to write the W3C `traceparent` / `tracestate` headers into each outgoing message. Pair this with `extractTraceContext` on the consumer side (see [Consumer helpers](#consumer-helpers--eventferrykafkaconsume)).

```ts
import { context as otelContext, propagation, trace } from "@opentelemetry/api";

const tracer: KafkaTracer = {
  startPublishSpan: /* …as above… */,
  inject(_span, headers) {
    // The publisher wraps the active span context for us before calling this.
    propagation.inject(otelContext.active(), headers);
  },
};
```

The publisher clones each outbound message before injecting (the caller's `PublishableMessage` is never mutated, so the relay's retry path stays correct).

## Power-user escape hatches

When the high-level options don't reach a knob you need, drop down to the native client config.

### Compression level

```ts
new KafkaPublisher({
  brokers,
  driver: "confluent",
  compression: "zstd",
  compressionLevel: 9, // librdkafka compression.level
});
```

Confluent only. The kafkajs driver logs a one-time warning and ignores it (kafkajs does not expose codec levels). Default level is the codec's broker-friendly default.

### Raw librdkafka producer config (confluent driver)

```ts
new KafkaPublisher({
  brokers,
  driver: "confluent",
  rawProducerConfig: {
    "queue.buffering.max.messages": 100_000,
    "statistics.interval.ms": 5_000,
    "socket.keepalive.enable": true,
  },
});
```

Merged on TOP of eventferry's translated config — raw keys **win** against the translated ones. Use this to override defaults (set `linger.ms` directly) or to tune surface area we don't expose (`queue.buffering.max.kbytes`, etc.).

### Raw kafkajs producer config (kafkajs driver)

```ts
new KafkaPublisher({
  brokers,
  driver: "kafkajs",
  rawKafkaJsProducerConfig: {
    retry: { retries: 7, initialRetryTime: 250 },
    metadataMaxAge: 5_000,
  },
});
```

Same precedence — raw keys win. Use for kafkajs-internal knobs (`retry`, `metadataMaxAge`) or to override defaults like `idempotent`.

### Custom partitioner (kafkajs driver)

```ts
const tenantAwarePartitioner = () => ({ topic, partitionMetadata, message }) => {
  const tenant = message.headers["x-tenant"]?.toString();
  return hashToPartition(tenant, partitionMetadata.length);
};

new KafkaPublisher({
  brokers,
  driver: "kafkajs",
  customPartitioner: tenantAwarePartitioner,
});
```

Overrides the `partitioner` preset. Confluent ignores this — librdkafka's partitioner is a C-level extension point, not a JS callback.

### Logger

Pass a `Logger` (the same interface used by `@eventferry/core`) to route the publisher's own diagnostics — driver warnings, hook failures — through your logging stack:

```ts
new KafkaPublisher({
  brokers,
  logger: pinoLoggerAdapter, // anything implementing { debug, info, warn, error }
});
```

When omitted, the publisher is silent and the driver falls back to `console.warn` for its diagnostics (preserves prior behavior).

## Admin operations

The publisher exposes a typed admin surface for listing/describing/creating topics — handy for provisioning in CI, integration tests, or app boot.

### `publisher.admin()`

Borrow a fresh admin client. The returned client is connected and ready; the **caller** is responsible for closing it.

```ts
const admin = await publisher.admin();
try {
  const topics = await admin.listTopics();
  const desc = await admin.describeTopics(["orders"]);
  console.log(desc[0].partitions.length); // partition count
} finally {
  await admin.close();
}
```

Methods on the returned `KafkaAdmin`:

- `listTopics(): Promise<string[]>` — all topic names visible to this principal.
- `describeTopics(topics): Promise<TopicMetadata[]>` — partition / leader / ISR per topic. Missing topics come back with an empty `partitions` array (no try/catch needed to detect absence).
- `createTopics(specs)` — idempotent: existing topics are silently skipped.
- `createPartitions(specs)` — grow a topic's partition count (Kafka does not support shrinking).
- `close()` — disconnect.

### `publisher.ensureTopics()`

One-shot, idempotent provisioning built on top of the admin surface:

```ts
await publisher.ensureTopics([
  { topic: "orders", numPartitions: 12, replicationFactor: 3 },
  { topic: "orders.dlq", numPartitions: 3, replicationFactor: 3, configEntries: { "retention.ms": "604800000" } },
]);

// Optionally grow existing topics whose partition count is below the requested numPartitions:
await publisher.ensureTopics(
  [{ topic: "orders", numPartitions: 24 }],
  { growPartitions: true },
);
```

What it does:

- Creates topics that don't exist.
- Skips topics that already exist (no error, no surprise alter).
- With `growPartitions: true`, calls `createPartitions` for existing topics whose current partition count is **below** the requested `numPartitions`.

What it does NOT do (by design):

- Reconcile replication factor on existing topics — Kafka has no safe in-place alter (use partition reassignment for that).
- Reconcile `configEntries` on existing topics — use `kafka-configs.sh` or the raw admin client (kafkajs's `alterConfigs`) if you need that.

### Consumer helpers — `@eventferry/kafka/consume`

eventferry is publisher-only, but the records it produces are consumed somewhere downstream. The `consume` subpath ships zero-dep helpers for the typical decode + trace-continuation glue, and pulls in no kafkajs/confluent code:

```ts
import { decode, extractTraceContext } from "@eventferry/kafka/consume";

await consumer.run({
  eachMessage: async ({ message }) => {
    // Normalize key/headers/value; default decoder is JSON.
    const { key, value, headers, offset } = decode<{ orderId: string }>(message);

    // Continue the producer's W3C trace context (if the publisher's tracer
    // injects it — see "OpenTelemetry tracing" above for the inject hook).
    const trace = extractTraceContext(message.headers);
    if (trace) {
      // → start a CONSUMER span as a child of trace.traceId / trace.spanId
    }

    await handle(value!);
  },
});
```

`decode` options:

- `decoder: "json"` (default) — `JSON.parse(value.toString("utf8"))`. Empty/null value → `null` (handles compaction tombstones).
- `decoder: "utf8"` — raw text.
- `decoder: "none"` — raw `Buffer`.
- `decoder: (bytes) => …` — custom (Avro, Protobuf, MessagePack, …).

`extractTraceContext` returns `null` if no `traceparent` header is present or it fails W3C validation (all-zero IDs, `version: ff`, malformed hex). It accepts both raw consumer headers (Buffer values) and already-decoded headers (string values).

### `validateTopicsOnConnect`

Fail-fast at startup if expected topics are missing:

```ts
new KafkaPublisher({
  brokers,
  validateTopicsOnConnect: ["orders", "orders.dlq", "events"],
});
```

`connect()` opens an admin, runs `listTopics`, and throws a single descriptive error naming **every** missing topic. The admin is always closed (success or failure). Skip the check entirely with an empty list or by omitting the option.

📖 **Full documentation:** [github.com/SametGoktepe/eventferry](https://github.com/SametGoktepe/eventferry#readme)

## License

MIT
