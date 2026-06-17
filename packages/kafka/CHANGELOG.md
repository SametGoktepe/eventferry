# @eventferry/kafka

## 3.4.0

### Minor Changes

- 3e0c5ee: Add typed admin surface to `KafkaPublisher`: `publisher.admin()` borrows a connected admin client (caller closes it), `publisher.ensureTopics()` idempotently provisions topics with an optional `growPartitions` flag, and a new `validateTopicsOnConnect` option fails fast at startup when expected topics are missing. Implemented on both the kafkajs and confluent drivers; custom drivers that don't implement the optional `admin()` method get a clear error message instead of a silent surprise.
- ac7a964: Add consumer-side helpers via the new `@eventferry/kafka/consume` subpath import: `decode(message, { decoder })` normalizes the raw message shape (key, value, headers, offset, timestamp, partition) both kafkajs and confluent deliver — with built-in `json` / `utf8` / `none` decoders plus a custom-function escape hatch; `extractTraceContext(headers)` parses the W3C `traceparent` / `tracestate` headers (strict validation per the W3C Trace Context spec) and accepts both raw (Buffer) and decoded (string) header shapes. Paired on the producer side with a new optional `KafkaTracer.inject(span, headers)` hook so OpenTelemetry users can complete the publish→consume trace propagation in two lines. The publisher clones each message before invoking `inject` — the caller's `PublishableMessage` references are never mutated, keeping the relay's retry path safe.
- 4007a8e: Power-user escape hatches for both drivers. The high-level options cover ~95% of cases; these let you reach into the native client when you need a knob we don't expose typed.

  - `compressionLevel`: per-codec level (confluent only, e.g. `zstd` level 1-22). Maps to librdkafka's `compression.level`. The kafkajs driver warns once and ignores it (kafkajs has no codec-level config).
  - `rawProducerConfig`: raw librdkafka keys merged into the confluent producer config. Native keys **win** against eventferry's translated ones — use this to override defaults or to tune surface area (queue buffering, statistics interval, socket keepalive, …) we don't expose.
  - `rawKafkaJsProducerConfig`: same idea for kafkajs — raw keys merged into `kafka.producer({...})` with last-write-wins precedence.
  - `customPartitioner`: kafkajs partitioner factory (`() => (args) => number`). Overrides the `partitioner` preset entirely. Confluent ignores it — librdkafka's partitioner is a C-level extension point.

  Native config takes precedence over eventferry's translated keys in every case — that's the contract of an escape hatch.

## 3.3.1

### Patch Changes

- 3c33f71: **chore: ship `CHANGELOG.md` inside the npm tarball**

  Previously, each package's `files` allowlist contained only `"dist"` (and `"sql"` for `@eventferry/postgres`), so the auto-generated `CHANGELOG.md` was never published. Users browsing the package on npmjs.com or unpacking the tarball couldn't see release notes — they had to navigate to the GitHub repo.

  This release adds `"CHANGELOG.md"` to the `files` array of every publishable package. Starting with this version, the per-version release notes are accessible:

  - Directly in `node_modules/@eventferry/<pkg>/CHANGELOG.md` after `npm install`
  - In the file listing on npmjs.com (under the "Code" / "Files" tab, depending on the npm UI)
  - Inside the tarball downloaded from `https://registry.npmjs.org/...`

  No code or API surface changes.

- Updated dependencies [3c33f71]
  - @eventferry/core@3.3.1

## 3.3.0

### Minor Changes

- cdc20cf: **feat: DLQ enrichment + backpressure runtime + quota multiplier — Tier 1 of the reliability gap closed**

  ### DLQ enrichment

  Records routed to the dead-letter queue now carry the full context an operator needs to triage:

  | Header                      | Set by    | Note                                                                                             |
  | --------------------------- | --------- | ------------------------------------------------------------------------------------------------ |
  | `original-topic`            | relay     | already existed                                                                                  |
  | `dlq-reason`                | publisher | already existed (`error.message`)                                                                |
  | `dlq-failed-at`             | publisher | already existed (ISO timestamp)                                                                  |
  | `dlq-error-class`           | publisher | **new** — `error.name` / constructor name                                                        |
  | `dlq-attempts`              | relay     | **new** — string-encoded `attempts` count                                                        |
  | `dlq-original-aggregate-id` | relay     | **new** — for joining with business state                                                        |
  | `dlq-original-message-id`   | relay     | **new** — for dedup / idempotency lookups                                                        |
  | `dlq-error-stack`           | relay     | **new** — opt-in via `DlqConfig.includeStackTraces`, truncated to `maxStackBytes` (default 4 KB) |

  ```ts
  new Relay({
    store,
    publisher,
    dlq: { topic: "orders.dlq", includeStackTraces: true, maxStackBytes: 4096 },
  });
  ```

  ### Backpressure runtime behavior

  When the driver classifies a failure as `errorKind: "backpressure"` (client-side producer queue full), the relay no longer treats it like a regular retriable failure. Instead:

  - The record is re-queued via the new `OutboxStore.requeue(id, retryAt)` method,
  - `attempts` is **not incremented** — the buffer being full is a "slow down" signal, not the record's fault,
  - The retry is scheduled `RetryConfig.backpressureDelayMs` ms ahead (default 1000 ms).

  Stores that don't implement `requeue` fall back to `markFailed` (with attempts++); both `@eventferry/postgres` and `@eventferry/mysql` ship a real implementation.

  ### Quota multiplier

  When the driver classifies a failure as `errorKind: "quota"` (broker `THROTTLING_QUOTA_EXCEEDED`), the scheduled retry delay is multiplied by `RetryConfig.quotaMultiplier` (default 5) so the producer gives the broker breathing room. Quota failures DO count as attempts — after the budget is exhausted the record routes to DLQ + `dead`.

  ### New / changed types

  - `RetryConfig` gains `backpressureDelayMs?` and `quotaMultiplier?`.
  - `DlqConfig` gains `includeStackTraces?` and `maxStackBytes?`.
  - `OutboxStore.requeue?(recordId, retryAt)` is a new **optional** method. Stores without it fall through to `markFailed`.

  ### Backward compatibility

  Pure-additive everywhere. Default behavior matches the prior release:

  - A `RetryConfig` without `backpressureDelayMs` uses 1000 ms (sensible default).
  - A `DlqConfig` without `includeStackTraces` keeps DLQ messages small (default off).
  - An `OutboxStore` without `requeue` falls back to `markFailed` — same as before, just with a documented quirk.

  This closes the last three Tier 1 items in `docs/kafka-gap-analysis/reliability.md`. Phase A reliability surface is now ~100% complete.

### Patch Changes

- Updated dependencies [cdc20cf]
  - @eventferry/core@3.3.0

## 3.2.1

### Patch Changes

- 9beb3e2: **chore: migrate to independent versioning (Astro pattern)**

  Fixes the major-version inflation that produced four consecutive surprise majors (`1.0.4 → 2.0.0`, `2.0.0 → 3.0.0`, `3.0.0 → 4.0.0 corrected to 3.1.0`, `3.1.0 → 4.0.0 corrected to 3.2.0`) from changesets whose frontmatter only asked for `minor`.

  **Root cause** (cited in [changesets/changesets#1759](https://github.com/changesets/changesets/issues/1759) and [docs/decisions.md](https://github.com/changesets/changesets/blob/main/docs/decisions.md)): the adapters listed `@eventferry/core` as a `peerDependency` with `workspace:*`. Changesets' documented rule is that an internal bump of a peer forces a major bump on the dependent — and the `fixed: [["@eventferry/*"]]` group reconciler then propagated that major across every package in the group.

  **Fix** (exactly the [Astro config](https://github.com/withastro/astro/blob/main/.changeset/config.json)):

  1. `.changeset/config.json` — drop `fixed`, set `linked: []`, enable
     `___experimentalUnsafeOptions_WILL_CHANGE_IN_PATCH.onlyUpdatePeerDependentsWhenOutOfRange: true`.
  2. Move `@eventferry/core` from `peerDependencies` to `dependencies` in
     `@eventferry/postgres`, `@eventferry/mysql`, `@eventferry/kafka`, and
     `@eventferry/schema-registry`. External user-facing peers (`pg`,
     `mysql2`, `kafkajs`, `@confluentinc/kafka-javascript`,
     `@kafkajs/confluent-schema-registry`) stay unchanged.

  **Effect on releases.** Packages now evolve at independent semver tempos: a `core: minor` changeset produces `core@3.3.0` alongside `postgres@3.2.1` (patch, from "Updated dependencies"). No more major surprises. No more manual force-push corrections.

  **Effect on consumers.** Pure-additive at the install boundary: `npm i @eventferry/kafka` now resolves `@eventferry/core` automatically (it's a regular dep). Previously consumers had to install it themselves as a peer; the typical flow already did this. No source-code changes required.

- Updated dependencies [9beb3e2]
  - @eventferry/core@3.2.1

## 3.2.0

### Minor Changes

- 0208275: **feat: OpenTelemetry publish span + hook surface + logger passthrough**

  ### OpenTelemetry tracing

  `KafkaPublisher` now accepts an optional `tracer` that follows the current stable [OpenTelemetry messaging semantic conventions](https://github.com/open-telemetry/semantic-conventions/blob/main/docs/messaging/kafka.md). One span per `publish()` call, named `"{topic} publish"`, with `messaging.system=kafka`, `messaging.operation.type=publish`, `messaging.destination.name=<topic>`, and `messaging.batch.message_count=<n>`. No dependency on `@opentelemetry/api` — wire through a 10-line adapter:

  ```ts
  import { trace, SpanKind, SpanStatusCode } from "@opentelemetry/api";
  import type { KafkaTracer } from "@eventferry/kafka";

  const otel = trace.getTracer("@eventferry/kafka");
  const tracer: KafkaTracer = {
    startPublishSpan(name, attributes) {
      const span = otel.startSpan(name, {
        kind: SpanKind.PRODUCER,
        attributes,
      });
      return {
        /* setAttribute, setStatus, recordException, end */
      };
    },
  };

  new KafkaPublisher({ brokers, tracer });
  ```

  ### Hook surface

  `KafkaPublisher` now accepts `hooks` for observability and metrics integration:

  ```ts
  new KafkaPublisher({
    brokers,
    hooks: {
      onConnect,
      onDisconnect,
      onPublish,
      onError,
      onTransactionAbort,
    },
  });
  ```

  Hooks are **safe by construction**: a throwing hook never breaks publishing — the publisher catches and logs via the configured `logger`.

  ### Logger passthrough

  A new optional `logger?: Logger` field on `KafkaPublisherOptions` (same `Logger` interface as `@eventferry/core`). Routes the publisher's own diagnostics (driver warnings about unsupported tuning, hook failures) through your logging stack instead of `console.warn`. When omitted, behavior matches today (drivers still fall back to `console.warn`).

  ### Backward compatibility

  100% additive. Existing call sites (no hooks, no tracer, no logger) work unchanged — the tracer defaults to a `NoopKafkaTracer`, the hook map defaults to `{}`, and the logger stays undefined.

- ae64a98: **feat: callable `transactionalId` + abort-aware tx hook**

  ### Callable `transactionalId`

  `transactionalId` accepts a sync or async resolver in addition to a plain string:

  ```ts
  new KafkaPublisher({
    brokers,
    transactional: true,
    transactionalId: () =>
      `${process.env.POD_NAME}-${process.env.REPLICA_INDEX}`,
  });
  ```

  Useful when the id depends on runtime context that isn't known at construction time (pod name, AZ + replica index, k8s ordinal). For multi-instance EOS, the resolved id MUST be stable across a single instance's restarts but UNIQUE across instances. The plain-string form remains supported and unchanged.

  ### Abort-aware `onTransactionAbort` hook

  When a transactional `sendBatch` triggers the abort path, the publisher fires `hooks.onTransactionAbort(err)` so dashboards and metrics catch EOS failure rates:

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

  Best-effort: the hook is safe-wrapped (a throwing hook never breaks the abort path); both `kafkajs` and `@confluentinc/kafka-javascript` drivers fire it from their transaction catch blocks.

  ### Backward compatibility

  100% additive. Existing call sites — string `transactionalId`, no hooks — work unchanged.

### Patch Changes

- @eventferry/core@3.2.0

## 3.1.0

### Minor Changes

- da39b08: **feat: producer tuning passthrough + per-message partition override + kafkajs partitioner choice**

  ### Producer tuning

  `KafkaPublisher` now accepts the full set of producer tuning knobs every serious Kafka deployment eventually needs:

  ```ts
  new KafkaPublisher({
    driver: "confluent",
    brokers,
    lingerMs: 25, // ⚠ confluent only
    batchSize: 131_072, // ⚠ confluent only
    maxInFlightRequests: 5,
    requestTimeoutMs: 30_000,
    deliveryTimeoutMs: 120_000, // ⚠ confluent only
    maxRequestSize: 2_000_000, // ⚠ confluent only
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

- bbb1792: **feat: mTLS + SASL/OAUTHBEARER support**

  Two new authentication paths for managed and enterprise Kafka clusters.

  ### mTLS (mutual TLS)

  The `ssl` option now accepts a full `TlsConfig` in addition to the boolean shorthand:

  ```ts
  new KafkaPublisher({
    brokers: ["broker:9093"],
    ssl: {
      ca: readFileSync("/etc/ssl/kafka-ca.pem"),
      cert: readFileSync("/etc/ssl/client.pem"),
      key: readFileSync("/etc/ssl/client-key.pem"),
      passphrase: "optional",
      servername: "broker.example.com", // SNI override
    },
  });
  ```

  Buffer and PEM-string inputs are both supported. `ssl: true` continues to work unchanged (one-way TLS using the driver's default trust store).

  > `rejectUnauthorized` is intentionally NOT exposed. TLS verification is non-negotiable; pass the cluster CA via `ca` for dev clusters with self-signed certs.

  ### SASL/OAUTHBEARER

  Required for Azure Event Hubs, Confluent Cloud with OAuth/SSO, and any OIDC-fronted cluster. Bring your own token provider:

  ```ts
  new KafkaPublisher({
    brokers: ["broker:9093"],
    ssl: true,
    sasl: {
      mechanism: "oauthbearer",
      oauthBearerProvider: async () => ({
        value: bearerToken,
        principal: "user@realm", // required on confluent
        lifetime: 3600_000, // ms — required on confluent
        extensions: { scope: "read,write" },
      }),
    },
  });
  ```

  **Driver asymmetry to know about:** `kafkajs` reads only `value`; `@confluentinc/kafka-javascript` requires `value` + `principal` + `lifetime` (ms) and accepts an optional `extensions` map. Cross-driver portable providers should populate all four.

  ### Confluent driver internals

  `@confluentinc/kafka-javascript` integrates via a small translator: simple `ssl: true` and SASL configs go through the kafkajs-compat layer, but a custom `TlsConfig` is mapped to the librdkafka PEM keys (`ssl.ca.pem`, `ssl.certificate.pem`, `ssl.key.pem`, `ssl.key.password`) and `security.protocol` is auto-derived (`ssl` / `sasl_plaintext` / `sasl_ssl`). Buffer inputs are coerced to UTF-8 strings (librdkafka does not accept Buffers).

  ### Backward compatibility

  Pure-additive. Existing configs (`ssl: true | false | undefined`, password SASL) work unchanged.

### Patch Changes

- Updated dependencies [da39b08]
  - @eventferry/core@3.1.0

## 3.0.0

### Minor Changes

- f0c7483: **feat: error classification for smarter retry, DLQ, and pause behavior**

  Publisher implementations can now tag each failed `PublishResult` with an `errorKind` so the relay knows whether the error is worth retrying.

  **New in `@eventferry/core`:**

  - `PublishErrorKind = "retriable" | "fatal" | "poison" | "backpressure" | "quota"` — opt-in classification surface on `PublishResult.errorKind`.
  - The `Relay` now reads `errorKind`:
    - `"fatal"` (auth denied, fenced epoch, transactional id rejected) and `"poison"` (oversized record, corrupt payload, schema rejected) **short-circuit retries** straight to the DLQ + `dead` status. No more burning the retry budget on errors that cannot succeed.
    - `"retriable"`, `"backpressure"`, `"quota"`, and absent (`undefined`) continue to use the existing backoff schedule, preserving backward compatibility. Smarter `backpressure` / `quota` handling (pause polling, longer backoff) is planned for a follow-up release.

  **New in `@eventferry/kafka`:**

  - `classifyKafkajsError(err): PublishErrorKind` — maps the most-common `KafkaJSProtocolError` types/codes and the `KafkaJSConnectionError` / `KafkaJSRequestTimeoutError` / `KafkaJSNonRetriableError` subclasses to a category. Verified against `kafkajs/src/errors.js`.
  - `classifyConfluentError(err): PublishErrorKind` — maps the librdkafka `RD_KAFKA_RESP_ERR_*` codes (both negative internal codes and Kafka wire-protocol codes) to a category. Verified against `librdkafka/src/rdkafka.h`. Includes the dedicated `"backpressure"` mapping for `ERR__QUEUE_FULL` (-184) and `"quota"` for `ERR_THROTTLING_QUOTA_EXCEEDED` (89).
  - Both drivers (`KafkaJsDriver`, `ConfluentDriver`) now call their respective classifier in the catch path and emit the `errorKind` on every failed `PublishResult`.

  **Backward compatibility:** `errorKind` is optional everywhere. Existing publisher implementations that don't set it continue to work unchanged — the relay treats absent `errorKind` as `"retriable"`, which is what the relay did before this change.

  **Migration:** none required.

### Patch Changes

- Updated dependencies [f0c7483]
  - @eventferry/core@3.0.0

## 2.0.0

### Patch Changes

- @eventferry/core@2.0.0

## 1.0.4

### Patch Changes

- Updated dependencies [64d115d]
  - @eventferry/core@1.0.4

## 1.0.3

### Patch Changes

- Updated dependencies [aaca9a2]
  - @eventferry/core@1.0.3

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
