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
}

export type DriverKind = "kafkajs" | "confluent";
