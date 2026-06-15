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

/** Shared connection config accepted by both drivers. */
export interface KafkaConnectionConfig {
  brokers: string[];
  clientId?: string;
  ssl?: boolean;
  sasl?: {
    mechanism: "plain" | "scram-sha-256" | "scram-sha-512";
    username: string;
    password: string;
  };
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
