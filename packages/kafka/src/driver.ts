import type { PublishableMessage, PublishResult } from "@eventferry/core";

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
  /** Required when transactional=true. Must be stable per producer instance. */
  transactionalId?: string;
  /** acks: -1/"all" (default), 0, or 1. */
  acks?: number;
  /** Compression codec. Driver maps to its native enum. */
  compression?: "none" | "gzip" | "snappy" | "lz4" | "zstd";

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
}

export type DriverKind = "kafkajs" | "confluent";
