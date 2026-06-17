---
"@eventferry/core": minor
"@eventferry/kafka": minor
---

**feat: producer tuning passthrough + per-message partition override + kafkajs partitioner choice**

### Producer tuning

`KafkaPublisher` now accepts the full set of producer tuning knobs every serious Kafka deployment eventually needs:

```ts
new KafkaPublisher({
  driver: "confluent",
  brokers,
  lingerMs: 25,        // ⚠ confluent only
  batchSize: 131_072,  // ⚠ confluent only
  maxInFlightRequests: 5,
  requestTimeoutMs: 30_000,
  deliveryTimeoutMs: 120_000,  // ⚠ confluent only
  maxRequestSize: 2_000_000,   // ⚠ confluent only
  transactionTimeoutMs: 90_000,
});
```

**Driver asymmetry:** `kafkajs` has no producer-level config for `lingerMs`, `batchSize`, `deliveryTimeoutMs`, or `maxRequestSize` — its batching is sticky-partitioner + hardcoded internals. The typed API stays uniform; on the kafkajs driver, those four knobs log a **one-time** warning (deduped process-wide) and are otherwise ignored. For fine-grained tuning, switch to the confluent driver.

### Per-message partition override

`PublishableMessage` gains an optional `partition?: number` field. When set, the publisher routes that record to the exact partition, bypassing the configured partitioner. Use cases: compacted topics with application-managed sharding, tenant-affinity routing, geo-pinning. Both drivers honor it.

### kafkajs partitioner choice

Silences the noisy `KafkaJSPartitionerNotSpecified` warning kafkajs v2 emits on every producer instance, by letting you pick a partitioner explicitly:

```ts
new KafkaPublisher({
  driver: "kafkajs",
  brokers,
  partitioner: "java-compatible", // (default) | "legacy" | "default"
});
```

- `"java-compatible"` is the new greenfield default (matches the Java client's murmur2).
- `"legacy"` preserves pre-v2 hash continuity for existing topics.
- `"default"` follows kafkajs's current default.

### Backward compatibility

Pure-additive. Existing call sites continue to work unchanged; the partitioner-choice default (`"java-compatible"`) is what kafkajs v2's migration guide recommends for new producers.
