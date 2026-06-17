import type { PublishableMessage, PublishResult } from "@eventferry/core";
import { classifyConfluentError } from "./confluent-classifier.js";
import { buildConfluentClientConfig } from "./confluent-config.js";
import { resolveTransactionalId } from "./transactional-id.js";
import type {
  KafkaConnectionConfig,
  KafkaDriver,
  ProducerBehaviorConfig,
} from "./driver.js";
import type {
  KafkaDriverAdmin,
  PartitionGrowSpec,
  TopicCreateSpec,
  TopicMetadata,
} from "./admin.js";

// Structural typing of the confluent KafkaJS-compatible API surface so this
// file compiles without the optional native dep installed.
interface CkProducer {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  send(args: unknown): Promise<unknown>;
  transaction(): Promise<CkTransaction>;
}
interface CkTransaction {
  send(args: unknown): Promise<unknown>;
  commit(): Promise<void>;
  abort(): Promise<void>;
}
interface CkKafka {
  producer(args?: unknown): CkProducer;
  admin(): CkAdmin;
}
// Structural shape of the confluent kafkaJS-compat admin client. The
// surface mirrors kafkajs's so we can reuse the wrapper pattern.
interface CkAdmin {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  listTopics(): Promise<string[]>;
  fetchTopicMetadata(args: { topics: string[] }): Promise<{
    topics: Array<{
      name: string;
      partitions: Array<{
        partitionId: number;
        leader: number;
        replicas: number[];
        isr: number[];
      }>;
    }>;
  }>;
  createTopics(args: {
    topics: Array<{
      topic: string;
      numPartitions?: number;
      replicationFactor?: number;
      configEntries?: Array<{ name: string; value: string }>;
    }>;
    waitForLeaders?: boolean;
  }): Promise<boolean>;
  createPartitions(args: {
    topicPartitions: Array<{ topic: string; count: number }>;
  }): Promise<void>;
}

export interface ConfluentDriverOptions
  extends KafkaConnectionConfig,
    ProducerBehaviorConfig {}

/**
 * Driver backed by `@confluentinc/kafka-javascript` (librdkafka wrapper).
 * Higher throughput; uses the KafkaJS-compatible promisified API so the
 * adapter mirrors the kafkajs driver closely.
 */
export class ConfluentDriver implements KafkaDriver {
  readonly transactional: boolean;
  private producer: CkProducer | null = null;
  private readonly opts: ConfluentDriverOptions;

  constructor(opts: ConfluentDriverOptions) {
    this.opts = opts;
    this.transactional = opts.transactional ?? false;
    if (this.transactional && !opts.transactionalId) {
      throw new Error(
        "ConfluentDriver: transactionalId is required when transactional=true",
      );
    }
  }

  async connect(): Promise<void> {
    this.producer = await this.createProducer();
    await this.producer.connect();
  }

  /**
   * Construct the underlying confluent producer. Overridable as a test seam so
   * the send/transaction logic can be exercised without a real broker.
   */
  protected async createProducer(): Promise<CkProducer> {
    const mod = await importConfluent();
    const { kafkaJS, librdkafka } = buildConfluentClientConfig(this.opts);
    const kafka: CkKafka = new mod.KafkaJS.Kafka({
      kafkaJS,
      ...librdkafka,
    });
    // Resolve a callable transactionalId — async-safe so runtime context
    // (pod name, AZ index, k8s ordinal) can be derived at connect time.
    const resolvedTxId = this.transactional
      ? await resolveTransactionalId(this.opts.transactionalId)
      : undefined;
    return kafka.producer({
      kafkaJS: {
        idempotent: this.opts.idempotent ?? true,
        ...(resolvedTxId ? { transactionalId: resolvedTxId } : {}),
      },
    });
  }

  async disconnect(): Promise<void> {
    await this.producer?.disconnect();
    this.producer = null;
  }

  /**
   * Construct a librdkafka-backed admin client wrapped in the eventferry
   * `KafkaDriverAdmin` shape. The publisher's `connect()` is called before
   * the admin reaches the user.
   */
  async admin(): Promise<KafkaDriverAdmin> {
    const mod = await importConfluent();
    const { kafkaJS, librdkafka } = buildConfluentClientConfig(this.opts);
    const kafka: CkKafka = new mod.KafkaJS.Kafka({ kafkaJS, ...librdkafka });
    return new ConfluentAdmin(kafka.admin());
  }

  async sendBatch(messages: PublishableMessage[]): Promise<PublishResult[]> {
    if (!this.producer) throw new Error("ConfluentDriver not connected");
    const topicMessages = groupByTopic(messages);
    const acks = this.opts.acks ?? -1;
    const compression = this.opts.compression;

    const doSends = async (target: CkProducer | CkTransaction) => {
      for (const tm of topicMessages) {
        await target.send({
          topic: tm.topic,
          messages: tm.messages,
          acks,
          ...(compression && compression !== "none" ? { compression } : {}),
        });
      }
    };

    if (this.transactional) {
      const txn = await this.producer.transaction();
      try {
        await doSends(txn);
        await txn.commit();
        return messages.map((m) => ({ recordId: m.recordId, ok: true }));
      } catch (err) {
        await txn.abort().catch(() => undefined);
        const error = err instanceof Error ? err : new Error(String(err));
        try {
          this.opts.onTransactionAbort?.(error);
        } catch {
          // swallow — abort hook is best-effort
        }
        return failedResults(messages, err);
      }
    }

    try {
      await doSends(this.producer);
      return messages.map((m) => ({ recordId: m.recordId, ok: true }));
    } catch (err) {
      return failedResults(messages, err);
    }
  }
}

function failedResults(
  messages: PublishableMessage[],
  err: unknown,
): PublishResult[] {
  const error = err instanceof Error ? err : new Error(String(err));
  const errorKind = classifyConfluentError(err);
  return messages.map((m) => ({
    recordId: m.recordId,
    ok: false,
    error,
    errorKind,
  }));
}

function groupByTopic(messages: PublishableMessage[]) {
  const byTopic = new Map<string, unknown[]>();
  for (const m of messages) {
    const arr = byTopic.get(m.topic) ?? [];
    arr.push({
      key: m.key,
      value: m.value,
      headers: m.headers,
      // Per-message partition override. librdkafka honors an explicit
      // partition value; undefined leaves the default partitioner in charge.
      ...(m.partition !== undefined ? { partition: m.partition } : {}),
    });
    byTopic.set(m.topic, arr);
  }
  return [...byTopic.entries()].map(([topic, msgs]) => ({
    topic,
    messages: msgs,
  }));
}

/**
 * Adapt the confluent (librdkafka) admin client to the
 * {@link KafkaDriverAdmin} contract. Idempotent semantics mirror the
 * kafkajs admin wrapper.
 */
class ConfluentAdmin implements KafkaDriverAdmin {
  constructor(private readonly client: CkAdmin) {}

  async connect(): Promise<void> {
    await this.client.connect();
  }

  async close(): Promise<void> {
    await this.client.disconnect();
  }

  async listTopics(): Promise<string[]> {
    return await this.client.listTopics();
  }

  async describeTopics(topics: string[]): Promise<TopicMetadata[]> {
    if (topics.length === 0) return [];
    const all = new Set(await this.client.listTopics());
    const existing = topics.filter((t) => all.has(t));
    const missing = topics.filter((t) => !all.has(t));
    const meta = existing.length
      ? await this.client.fetchTopicMetadata({ topics: existing })
      : { topics: [] };
    const byName = new Map(meta.topics.map((t) => [t.name, t]));
    return topics.map((topic) => {
      if (missing.includes(topic)) return { topic, partitions: [] };
      const found = byName.get(topic);
      if (!found) return { topic, partitions: [] };
      return {
        topic,
        partitions: found.partitions.map((p) => ({
          partitionId: p.partitionId,
          leader: p.leader,
          replicas: p.replicas,
          isr: p.isr,
        })),
      };
    });
  }

  async createTopics(specs: TopicCreateSpec[]): Promise<void> {
    if (specs.length === 0) return;
    const topics = specs.map((s) => ({
      topic: s.topic,
      numPartitions: s.numPartitions,
      replicationFactor: s.replicationFactor,
      configEntries: s.configEntries
        ? Object.entries(s.configEntries).map(([name, value]) => ({ name, value }))
        : undefined,
    }));
    try {
      await this.client.createTopics({ topics, waitForLeaders: true });
    } catch (err) {
      // librdkafka raises a `code` numeric `36` for TOPIC_ALREADY_EXISTS;
      // the kafkaJS-compat layer mirrors the kafkajs error shape too.
      const e = err as { code?: number; name?: string; message?: string };
      if (e?.code === 36 || e?.name === "TOPIC_ALREADY_EXISTS") return;
      if (/already exists/i.test(e?.message ?? "")) return;
      throw err;
    }
  }

  async createPartitions(specs: PartitionGrowSpec[]): Promise<void> {
    if (specs.length === 0) return;
    await this.client.createPartitions({
      topicPartitions: specs.map((s) => ({
        topic: s.topic,
        count: s.totalCount,
      })),
    });
  }
}

async function importConfluent(): Promise<{
  KafkaJS: { Kafka: new (cfg: unknown) => CkKafka };
}> {
  try {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return (await import("@confluentinc/kafka-javascript")) as any;
  } catch {
    throw new Error(
      'Driver "confluent" selected but "@confluentinc/kafka-javascript" is not installed. Run: npm i @confluentinc/kafka-javascript',
    );
  }
}
