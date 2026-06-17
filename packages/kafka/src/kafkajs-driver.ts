import type {
  Logger,
  PublishableMessage,
  PublishResult,
} from "@eventferry/core";
import { classifyKafkajsError } from "./kafkajs-classifier.js";
import { resolveTransactionalId } from "./transactional-id.js";
import type {
  KafkaConnectionConfig,
  KafkaDriver,
  KafkaJsPartitionerChoice,
  ProducerBehaviorConfig,
} from "./driver.js";
import type {
  KafkaDriverAdmin,
  PartitionGrowSpec,
  TopicCreateSpec,
  TopicMetadata,
} from "./admin.js";

// Loosely-typed structural references to the kafkajs API so this file
// compiles without kafkajs installed (it's an optional peer dep).
interface KjsProducer {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  // kafkajs: sendBatch takes { topicMessages }, send takes a single { topic, messages }.
  sendBatch(args: unknown): Promise<unknown>;
  transaction(): Promise<KjsTransaction>;
}
interface KjsTransaction {
  sendBatch(args: unknown): Promise<unknown>;
  commit(): Promise<void>;
  abort(): Promise<void>;
}
interface KjsKafka {
  producer(args?: unknown): KjsProducer;
  admin(): KjsAdmin;
}
// Minimal structural shape of the kafkajs Admin client — we use only the
// methods needed for listTopics / describeTopics / createTopics / createPartitions.
// Errors thrown from these calls bubble up unchanged (admin errors are
// operator-facing, not relay-facing — no error-kind classification here).
interface KjsAdmin {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  listTopics(): Promise<string[]>;
  fetchTopicMetadata(args: {
    topics: string[];
  }): Promise<{
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
// kafkajs's `Partitioners` namespace: three factory functions; we pluck them
// at runtime rather than depending on the kafkajs types.
interface KjsPartitionersNamespace {
  DefaultPartitioner: () => unknown;
  LegacyPartitioner: () => unknown;
  JavaCompatiblePartitioner: () => unknown;
}

export interface KafkaJsDriverOptions
  extends KafkaConnectionConfig,
    ProducerBehaviorConfig {
  /**
   * Optional logger for the driver's own diagnostics (e.g. warnings about
   * unsupported tuning options). When absent the driver falls back to
   * `console.warn` so existing users see the same output.
   */
  logger?: Logger;
}

/**
 * kafkajs producer-level knobs we expose on the typed API that kafkajs does
 * NOT actually support. On this driver these are warn-and-ignore; users
 * who need them should switch to the confluent driver.
 */
const UNSUPPORTED_BY_KAFKAJS = [
  "lingerMs",
  "batchSize",
  "deliveryTimeoutMs",
  "maxRequestSize",
] as const;

/**
 * Driver backed by the pure-JS `kafkajs` client. Simple, zero native deps.
 */
export class KafkaJsDriver implements KafkaDriver {
  readonly transactional: boolean;
  private producer: KjsProducer | null = null;
  private readonly opts: KafkaJsDriverOptions;

  constructor(opts: KafkaJsDriverOptions) {
    this.opts = opts;
    this.transactional = opts.transactional ?? false;
    if (this.transactional && !opts.transactionalId) {
      throw new Error(
        "KafkaJsDriver: transactionalId is required when transactional=true",
      );
    }
    warnUnsupportedKafkajsOptions(opts);
  }

  async connect(): Promise<void> {
    this.producer = await this.createProducer();
    await this.producer.connect();
  }

  /**
   * Construct the underlying kafkajs producer. Overridable as a test seam so
   * the send/transaction logic can be exercised without a real broker.
   */
  protected async createProducer(): Promise<KjsProducer> {
    const mod = await importKafkaJs();
    const kafka: KjsKafka = new mod.Kafka({
      clientId: this.opts.clientId ?? "eventferry",
      brokers: this.opts.brokers,
      // kafkajs accepts `ssl: tls.ConnectionOptions` directly — Buffer + PEM
      // string both supported. Our TlsConfig is a structural subset of that
      // (`rejectUnauthorized` intentionally omitted; the cluster CA goes via
      // `ca`). No translation needed.
      ssl: this.opts.ssl,
      // SASL: PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512 / OAUTHBEARER. kafkajs's
      // shape matches ours; for OAUTHBEARER kafkajs reads only `value` from
      // the provider's returned token (other fields are ignored).
      sasl: this.opts.sasl,
    });
    const createPartitioner = resolveCreatePartitioner(
      mod.Partitioners,
      this.opts.partitioner,
      this.transactional,
    );
    // Resolve a callable transactionalId — async-safe so runtime context
    // (pod name, AZ index, k8s ordinal) can be derived at connect time.
    const resolvedTxId = this.transactional
      ? await resolveTransactionalId(this.opts.transactionalId)
      : undefined;
    return kafka.producer({
      idempotent: this.opts.idempotent ?? true,
      // Idempotent / transactional producers cap maxInFlight at 5. When the
      // user picks transactional we force 1 to keep strict ordering across
      // retries on classic (non-idempotent) clusters that haven't migrated
      // to the broker-side fence.
      maxInFlightRequests: this.transactional
        ? 1
        : this.opts.maxInFlightRequests,
      transactionalId: resolvedTxId,
      // kafkajs accepts these directly when set; undefined falls through to
      // the kafkajs default.
      requestTimeout: this.opts.requestTimeoutMs,
      transactionTimeout: this.opts.transactionTimeoutMs,
      // Setting any partitioner choice silences kafkajs's
      // KafkaJSPartitionerNotSpecified warning.
      createPartitioner,
    });
  }

  async disconnect(): Promise<void> {
    await this.producer?.disconnect();
    this.producer = null;
  }

  /**
   * Construct a kafkajs admin client wrapped in the eventferry-facing
   * `KafkaDriverAdmin` shape. The publisher calls `.connect()` on the
   * returned object before exposing it via `publisher.admin()`.
   */
  async admin(): Promise<KafkaDriverAdmin> {
    const mod = await importKafkaJs();
    const kafka: KjsKafka = new mod.Kafka({
      clientId: this.opts.clientId ?? "eventferry-admin",
      brokers: this.opts.brokers,
      ssl: this.opts.ssl,
      sasl: this.opts.sasl,
    });
    return new KafkaJsAdmin(kafka.admin());
  }

  async sendBatch(messages: PublishableMessage[]): Promise<PublishResult[]> {
    if (!this.producer) throw new Error("KafkaJsDriver not connected");
    const topicMessages = groupByTopic(messages, this.opts.compression);

    if (this.transactional) {
      const txn = await this.producer.transaction();
      try {
        await txn.sendBatch({ topicMessages, acks: this.opts.acks ?? -1 });
        await txn.commit();
        return messages.map((m) => ({ recordId: m.recordId, ok: true }));
      } catch (err) {
        await txn.abort().catch(() => undefined);
        const error = err instanceof Error ? err : new Error(String(err));
        // Notify the abort hook BEFORE returning failedResults. The hook is
        // best-effort: try/catch around it so a misbehaving hook can't make
        // the abort path itself throw.
        try {
          this.opts.onTransactionAbort?.(error);
        } catch {
          // swallow — already documented as best-effort
        }
        return failedResults(messages, err);
      }
    }

    try {
      await this.producer.sendBatch({ topicMessages, acks: this.opts.acks ?? -1 });
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
  const errorKind = classifyKafkajsError(err);
  return messages.map((m) => ({
    recordId: m.recordId,
    ok: false,
    error,
    errorKind,
  }));
}

function groupByTopic(messages: PublishableMessage[], compression?: string) {
  const byTopic = new Map<string, unknown[]>();
  for (const m of messages) {
    const arr = byTopic.get(m.topic) ?? [];
    arr.push({
      key: m.key,
      value: m.value,
      headers: m.headers,
      // Per-message partition override. When set, kafkajs routes the record
      // to this exact partition; when undefined, the configured partitioner
      // chooses. We keep the key here too because compacted topics need it
      // even when partition is pinned.
      ...(m.partition !== undefined ? { partition: m.partition } : {}),
    });
    byTopic.set(m.topic, arr);
  }
  return [...byTopic.entries()].map(([topic, msgs]) => ({
    topic,
    messages: msgs,
    ...(compression && compression !== "none" ? { compression } : {}),
  }));
}

/**
 * Resolve the `createPartitioner` factory kafkajs expects on
 * `producer({...})`. Returns `undefined` to fall through to the kafkajs
 * default when no choice is made AND the producer is non-transactional
 * (transactional producers don't trigger the no-partitioner warning).
 */
function resolveCreatePartitioner(
  partitioners: KjsPartitionersNamespace | undefined,
  choice: KafkaJsPartitionerChoice | undefined,
  transactional: boolean,
): (() => unknown) | undefined {
  if (!partitioners) return undefined;
  // Default to the java-compatible partitioner when the caller didn't pick.
  // It matches the Java client (murmur2) and silences the noisy warning;
  // for transactional producers we leave the kafkajs default alone since
  // EOS ordering is partitioner-agnostic and the warning doesn't fire there.
  const effective: KafkaJsPartitionerChoice =
    choice ?? (transactional ? "default" : "java-compatible");
  switch (effective) {
    case "java-compatible":
      return partitioners.JavaCompatiblePartitioner;
    case "legacy":
      return partitioners.LegacyPartitioner;
    case "default":
      return partitioners.DefaultPartitioner;
  }
}

/** Process-wide dedup so we never warn for the same option twice. */
const warnedKafkajsKeys = new Set<string>();

function warnUnsupportedKafkajsOptions(opts: KafkaJsDriverOptions): void {
  for (const key of UNSUPPORTED_BY_KAFKAJS) {
    if (opts[key] === undefined) continue;
    if (warnedKafkajsKeys.has(key)) continue;
    warnedKafkajsKeys.add(key);
    const message =
      `'${key}' is not configurable on the kafkajs driver and was ignored. ` +
      `Switch to the confluent driver (driver: "confluent") for fine-grained tuning, ` +
      `or remove the option to silence this warning.`;
    // Route through the configured logger when present; otherwise fall back
    // to console.warn so users who never plumbed a logger still see the
    // diagnostic (matches the prior behavior).
    if (opts.logger) {
      opts.logger.warn(`[@eventferry/kafka] ${message}`, { option: key });
    } else {
      console.warn(`[@eventferry/kafka] ${message}`);
    }
  }
}

/** Internal — used by tests. Resets the dedup so warnings can be observed in isolation. */
export function _resetKafkajsWarnDedup(): void {
  warnedKafkajsKeys.clear();
}

/**
 * Adapt the kafkajs admin client to the {@link KafkaDriverAdmin} contract.
 * Idempotent semantics for `createTopics` / `createPartitions` are
 * implemented here so the publisher's `ensureTopics` helper stays small.
 */
class KafkaJsAdmin implements KafkaDriverAdmin {
  constructor(private readonly client: KjsAdmin) {}

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
    // Fetch metadata only for the ones that exist; map absent topics to
    // empty-partitions descriptors (matches the documented behavior).
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
      // kafkajs raises KafkaJSProtocolError with type "TOPIC_ALREADY_EXISTS".
      // Idempotent contract: silently swallow that case; surface everything else.
      const e = err as { type?: string; message?: string };
      if (e?.type === "TOPIC_ALREADY_EXISTS") return;
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

async function importKafkaJs(): Promise<{
  Kafka: new (cfg: unknown) => KjsKafka;
  Partitioners: KjsPartitionersNamespace;
}> {
  try {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return (await import("kafkajs")) as any;
  } catch {
    throw new Error(
      'Driver "kafkajs" selected but the "kafkajs" package is not installed. Run: npm i kafkajs',
    );
  }
}
