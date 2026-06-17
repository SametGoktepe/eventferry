import type { PublishableMessage, PublishResult } from "@eventferry/core";
import type { KafkaDriverAdmin } from "./admin.js";

/**
 * Low-level driver contract. Each concrete driver (kafkajs, confluent)
 * adapts its native client to this minimal surface. The KafkaPublisher
 * orchestrates batching, transactions and DLQ on top of it.
 */
export interface KafkaDriver {
  connect(): Promise<void>;
  disconnect(): Promise<void>;

  /**
   * Send a batch of records. The driver should preserve ordering for
   * messages that share the same key (Kafka guarantees this per-partition).
   * Returns one result per input message.
   */
  sendBatch(messages: PublishableMessage[]): Promise<PublishResult[]>;

  /**
   * Whether this driver supports transactional (EOS) sends. The publisher
   * uses this to decide whether `sendBatch` is atomic.
   */
  readonly transactional: boolean;

  /**
   * Construct a NEW admin client. The returned admin is not yet connected —
   * the publisher calls `.connect()` before handing it to the user.
   *
   * Optional: drivers without an admin surface may omit this; the publisher
   * throws a clear error when `publisher.admin()` is called on such a driver.
   */
  admin?(): Promise<KafkaDriverAdmin>;
}

/**
 * TLS configuration for client connections. Pass a full {@link TlsConfig}
 * when the cluster requires CA pinning, mutual TLS (client cert + key), or a
 * specific SNI host. Plain `ssl: true` keeps the previous behavior (one-way
 * TLS using the driver's default trust store).
 *
 * `rejectUnauthorized` is intentionally NOT a knob here — TLS verification is
 * non-negotiable. Dev clusters with self-signed certs pass their CA via `ca`.
 */
export interface TlsConfig {
  /** PEM-encoded CA bundle. Buffers and strings both accepted. */
  ca?: string | Buffer | Array<string | Buffer>;
  /** PEM-encoded client certificate (required for mTLS). */
  cert?: string | Buffer;
  /** PEM-encoded private key for the client certificate (required for mTLS). */
  key?: string | Buffer;
  /** Passphrase for an encrypted private key. */
  passphrase?: string;
  /** SNI host. Useful when broker address doesn't match the cert SAN. */
  servername?: string;
}

/**
 * Username + password SASL: PLAIN and SCRAM-SHA-256/512. The conventional
 * "API key + secret" shape used by Confluent Cloud, Aiven, on-prem SCRAM.
 */
export interface SaslPasswordConfig {
  mechanism: "plain" | "scram-sha-256" | "scram-sha-512";
  username: string;
  password: string;
}

/**
 * Token returned by an OAUTHBEARER provider.
 *
 * Driver asymmetry (verified against `kafkajs/types/index.d.ts` and
 * `@confluentinc/kafka-javascript/types/kafkajs.d.ts`):
 *
 * - `kafkajs` reads only `value`. Other fields are silently ignored.
 * - `@confluentinc/kafka-javascript` REQUIRES `value` + `principal` + `lifetime`
 *   and accepts an optional `extensions` map. Passing only `{ value }` throws.
 *
 * Cross-driver portable providers MUST populate all four. eventferry treats
 * `principal` / `lifetime` / `extensions` as optional in the type to support
 * kafkajs-only setups; supplying them is a no-op there.
 */
export interface OauthBearerToken {
  /** The bearer token string (JWT, opaque, …). */
  value: string;
  /** Principal name. REQUIRED on the confluent driver. */
  principal?: string;
  /** Lifetime in MILLISECONDS. REQUIRED on the confluent driver. */
  lifetime?: number;
  /** SASL extensions to send alongside the token (e.g. for OIDC scopes). */
  extensions?: Record<string, string>;
}

/**
 * SASL/OAUTHBEARER: bring-your-own token provider. The function is invoked
 * by the underlying client on demand (NOT on a fixed timer); cache the
 * token in your provider if you want to amortise issuance cost.
 *
 * Required for Azure Event Hubs, Confluent Cloud with OAuth/SSO, and any
 * OIDC-fronted Kafka. For AWS MSK IAM, wrap the AWS SigV4 signer in this
 * callback.
 */
export interface SaslOauthbearerConfig {
  mechanism: "oauthbearer";
  oauthBearerProvider: () => Promise<OauthBearerToken>;
}

/**
 * Discriminated union over the SASL mechanisms eventferry supports today.
 * Add new mechanisms by extending this union and mapping them in each driver.
 */
export type SaslConfig = SaslPasswordConfig | SaslOauthbearerConfig;

/** Shared connection config accepted by both drivers. */
export interface KafkaConnectionConfig {
  brokers: string[];
  clientId?: string;
  /**
   * TLS configuration. `true` enables one-way TLS using the driver's default
   * trust store; a {@link TlsConfig} object lets you supply a custom CA,
   * client cert (for mTLS), and SNI host.
   */
  ssl?: boolean | TlsConfig;
  sasl?: SaslConfig;
}

/**
 * Choice of partitioner. Only honored by the kafkajs driver — the confluent
 * driver uses librdkafka's `consistent_random` (key-aware sticky) and
 * partitioner override is out of scope for this release.
 *
 * - `"java-compatible"` (recommended for greenfield): kafkajs's
 *   `Partitioners.JavaCompatiblePartitioner`. Matches the Java client's
 *   murmur2-based hash so producers across language boundaries land on the
 *   same partition for the same key.
 * - `"legacy"`: kafkajs's pre-v2 partitioner. Use when migrating an existing
 *   topic where hash continuity matters.
 * - `"default"`: kafkajs's current default. Equivalent to legacy in v2 but
 *   may change with major kafkajs releases.
 *
 * Setting this also silences the noisy `KafkaJSPartitionerNotSpecified`
 * warning kafkajs emits when no partitioner choice is made explicitly.
 */
export type KafkaJsPartitionerChoice = "default" | "legacy" | "java-compatible";

export interface ProducerBehaviorConfig {
  /** Enable idempotent producer (dedup + ordering). Default true. */
  idempotent?: boolean;
  /**
   * Enable transactional producer for atomic batch publishing (EOS).
   * Requires a stable transactionalId. Default false.
   */
  transactional?: boolean;
  /**
   * Required when `transactional=true`. Must be stable per producer instance
   * — two producers sharing the same id race for the broker-side epoch and
   * fence each other.
   *
   * Accepts a string OR a thunk that resolves the id at connect time. The
   * callable form lets you derive the id from runtime context that's not
   * known at construction (pod name, AZ + replica index, k8s ordinal):
   *
   *     transactionalId: () => `${process.env.POD_NAME}-${replicaIndex()}`,
   *
   * For multi-instance EOS, the derived id MUST be stable across a single
   * instance's restarts but UNIQUE across instances.
   */
  transactionalId?: string | (() => string | Promise<string>);
  /** acks: -1/"all" (default), 0, or 1. */
  acks?: number;
  /** Compression codec. Driver maps to its native enum. */
  compression?: "none" | "gzip" | "snappy" | "lz4" | "zstd";
  /**
   * (confluent only) Compression level for the chosen codec. Defaults vary
   * per codec — librdkafka picks the broker-friendly default when unset.
   * Common ranges: gzip 1–9, lz4 0–12, zstd 1–22 (higher = smaller + slower).
   *
   * No-op on the kafkajs driver (kafkajs does not expose codec levels).
   */
  compressionLevel?: number;

  // ── Tuning knobs ────────────────────────────────────────────────────────
  //
  // Driver asymmetry: `kafkajs` does NOT expose `lingerMs`, `batchSize`,
  // `deliveryTimeoutMs`, or `maxRequestSize` as producer config — its
  // batching is sticky-partitioner + hardcoded internals. The publicly
  // typed API stays uniform; on the kafkajs driver, the four
  // librdkafka-only knobs log a one-time warning and are otherwise
  // ignored. Use the confluent driver for fine-grained tuning.

  /**
   * (confluent only) How long the producer waits to accumulate records before
   * flushing a partition batch. Default 0 (ship-immediately). Increase to
   * 10–50ms for higher throughput at the cost of latency.
   */
  lingerMs?: number;
  /** (confluent only) Maximum bytes per partition batch before forced flush. */
  batchSize?: number;
  /**
   * Max concurrent unacknowledged producer requests. MUST be ≤5 when
   * `idempotent: true`. Higher = throughput; lower = stricter ordering on
   * non-idempotent producers (no other path preserves order on retry).
   */
  maxInFlightRequests?: number;
  /** Per-request broker-ack timeout. Default 30 s. */
  requestTimeoutMs?: number;
  /**
   * (confluent only) End-to-end timeout for a record from produce() call to
   * terminal success / failure (includes retries). Defaults to 120 s.
   * If this exceeds the relay's `claimTimeoutMs`, the reaper may double-
   * publish a slow record — set both coherently.
   */
  deliveryTimeoutMs?: number;
  /**
   * (confluent only) Max bytes of a single record (after compression).
   * MUST be ≤ broker's `message.max.bytes`. Defaults to 1 MB.
   */
  maxRequestSize?: number;
  /**
   * Broker-side ceiling on how long a transaction can stay open before
   * auto-abort. Maps to `transaction.timeout.ms`. Default 60 s; capped by
   * the broker's `transaction.max.timeout.ms`.
   */
  transactionTimeoutMs?: number;
  /**
   * (kafkajs only) Choice of partitioner. See
   * {@link KafkaJsPartitionerChoice} for the options. Setting any value
   * silences kafkajs's `KafkaJSPartitionerNotSpecified` warning.
   */
  partitioner?: KafkaJsPartitionerChoice;
  /**
   * Callback fired when a transactional `sendBatch` triggers the abort
   * path (e.g. mid-batch driver error, broker rejection). Used by the
   * publisher to fan out the matching `KafkaPublisherHooks.onTransactionAbort`
   * hook — but advanced users constructing a driver directly may also wire
   * it themselves. Best-effort: the driver still proceeds to abort the
   * underlying transaction and return per-record failures regardless of
   * whether this callback throws.
   */
  onTransactionAbort?: (error: Error) => void;

  // ── Power-user escape hatches ───────────────────────────────────────────
  //
  // The high-level options above cover ~95% of cases. The hooks below let
  // you reach into the native client when you need a knob we don't expose.
  // Native config takes PRECEDENCE over eventferry's translated keys —
  // anything you put here wins. Use sparingly; surfaces are NOT typed.

  /**
   * (confluent only) Raw librdkafka producer-config keys merged on top of
   * eventferry's translated config. Use for tuning surface area we don't
   * expose typed (e.g. `queue.buffering.max.messages`, `socket.keepalive.enable`,
   * `statistics.interval.ms`). Native keys win against the translated ones,
   * so this can also be used to override defaults.
   *
   * Ignored by the kafkajs driver — log a one-time warning instead of
   * silently dropping. Use `rawKafkaJsProducerConfig` for kafkajs-side tuning.
   */
  rawProducerConfig?: Record<string, unknown>;
  /**
   * (kafkajs only) Raw producer-config keys merged into kafkajs's
   * `kafka.producer({...})` call. Native keys win against the translated
   * ones. Use for kafkajs-internal knobs like `retry`, `metadataMaxAge`,
   * `idempotent` overrides, etc.
   *
   * No-op on the confluent driver — use `rawProducerConfig` there.
   */
  rawKafkaJsProducerConfig?: Record<string, unknown>;
  /**
   * (kafkajs only) Custom partitioner factory passed straight to
   * `kafka.producer({ createPartitioner })`. Overrides {@link partitioner}
   * preset entirely. See kafkajs docs for the factory signature:
   * `() => (args: { topic, partitionMetadata, message }) => number`.
   *
   * Ignored by the confluent driver — librdkafka's partitioner is a C
   * extension point, not a JS callback.
   */
  customPartitioner?: () => (args: unknown) => number;
  /**
   * (confluent only) Periodic librdkafka statistics callback. When set,
   * eventferry wires `stats_cb` on the underlying producer and parses the
   * JSON payload librdkafka emits every {@link statsIntervalMs} ms.
   *
   * The shape is intentionally opaque — librdkafka's stats schema is huge
   * (txmsgs, rxbytes, queue depth, broker timeouts, per-topic / per-partition
   * counters…) and evolves across versions. Documented at
   * https://github.com/confluentinc/librdkafka/blob/master/STATISTICS.md.
   * Cast to your own narrower type if you're consuming a known subset.
   *
   * No-op on the kafkajs driver — kafkajs has no equivalent surface.
   * Pair with {@link statsIntervalMs} (defaults to 30000 ms when this hook
   * is set but `rawProducerConfig['statistics.interval.ms']` isn't).
   */
  onStats?: (stats: LibrdkafkaStats) => void;
  /**
   * (confluent only) Override the polling interval the librdkafka stats
   * callback fires at. Maps to `statistics.interval.ms`. Defaults to
   * 30000 ms when {@link onStats} is set; defaults to 0 (disabled)
   * otherwise — librdkafka spends CPU on this and we don't want to enable
   * it silently. Set to 0 to suppress emission while keeping the hook
   * defined (useful for tests).
   */
  statsIntervalMs?: number;
}

/**
 * Opaque envelope for librdkafka's stats JSON. The schema is
 * version-specific and large; eventferry surfaces it untyped so you can
 * cast to whatever subset you care about.
 *
 * Reference: https://github.com/confluentinc/librdkafka/blob/master/STATISTICS.md
 */
export type LibrdkafkaStats = Record<string, unknown>;

export type DriverKind = "kafkajs" | "confluent";
