/**
 * Admin surface for `KafkaPublisher`. A typed wrapper over each driver's
 * underlying admin client — implementations live in `kafkajs-driver.ts` and
 * `confluent-driver.ts`. The publisher exposes this via `publisher.admin()`
 * (returns a connected admin) and `publisher.ensureTopics()` (idempotent
 * topic provisioning built on top).
 *
 * Scope is deliberately the most-used subset of the Kafka AdminClient
 * protocol — listing, describing, creating topics and partitions. ACL
 * management, quota inspection, and consumer-group operations are left to
 * the underlying client (reach for kafkajs's `Admin` directly if needed).
 */

/** Specification for creating one topic. */
export interface TopicCreateSpec {
  topic: string;
  /** Default: cluster's `num.partitions` broker setting. */
  numPartitions?: number;
  /** Default: cluster's `default.replication.factor` broker setting. */
  replicationFactor?: number;
  /**
   * Per-topic config entries (e.g. `{ "retention.ms": "604800000" }`).
   * See Kafka broker docs for the full set.
   */
  configEntries?: Record<string, string>;
}

/** Topic + partition descriptor returned by describeTopics. */
export interface TopicMetadata {
  topic: string;
  /** Empty when the topic doesn't exist (so callers can detect absence cheaply). */
  partitions: PartitionMetadata[];
}

export interface PartitionMetadata {
  partitionId: number;
  /** Broker id of the partition leader; -1 if no leader is known. */
  leader: number;
  /** Replica broker ids. */
  replicas: number[];
  /** In-sync replica broker ids. */
  isr: number[];
}

/** Specification for growing a topic's partition count (never shrink). */
export interface PartitionGrowSpec {
  topic: string;
  /** Total partition count after the change. MUST be ≥ the current count. */
  totalCount: number;
}

/**
 * Typed admin surface exposed by {@link KafkaPublisher.admin}.
 *
 * The returned object is already connected — call `.close()` (or let the
 * publisher's `disconnect()` cascade close it) when done.
 *
 * All methods may throw native errors from the underlying client. The
 * publisher does not classify admin errors via the `errorKind` machinery
 * because admin failures are operator-facing and don't flow through the
 * relay's retry path.
 */
export interface KafkaAdmin {
  /** All topic names visible to this principal, including internal topics. */
  listTopics(): Promise<string[]>;

  /**
   * Metadata for the given topics. Topics that don't exist on the cluster
   * are returned with an empty `partitions` array — callers detect absence
   * cheaply without a try/catch.
   */
  describeTopics(topics: string[]): Promise<TopicMetadata[]>;

  /**
   * Create topics. Idempotent at this layer: topics that already exist are
   * silently skipped (the underlying client may throw `TopicExistsError`;
   * we swallow it). Use `ensureTopics` on the publisher for partition-count
   * + replication-factor coherence checks.
   */
  createTopics(specs: TopicCreateSpec[]): Promise<void>;

  /**
   * Grow each topic's partition count. Never shrinks (Kafka does not
   * support shrinking partitions). Specs whose `totalCount` equals the
   * current partition count are silently skipped.
   */
  createPartitions(specs: PartitionGrowSpec[]): Promise<void>;

  /** Disconnect the admin client. */
  close(): Promise<void>;
}

/**
 * Optional driver-level hook returned by {@link KafkaDriver.admin}. Drivers
 * implement this thin contract; the publisher composes the higher-level
 * `KafkaAdmin` and `ensureTopics` on top.
 */
export interface KafkaDriverAdmin extends KafkaAdmin {
  /** Called by the publisher before any method is invoked. */
  connect(): Promise<void>;
}
