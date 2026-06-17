import type { PublishableMessage, PublishResult } from "@eventferry/core";
import { classifyKafkajsError } from "./kafkajs-classifier.js";
import type {
  KafkaConnectionConfig,
  KafkaDriver,
  ProducerBehaviorConfig,
} from "./driver.js";

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
}

export interface KafkaJsDriverOptions
  extends KafkaConnectionConfig,
    ProducerBehaviorConfig {}

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
    return kafka.producer({
      idempotent: this.opts.idempotent ?? true,
      maxInFlightRequests: this.transactional ? 1 : undefined,
      transactionalId: this.transactional ? this.opts.transactionalId : undefined,
    });
  }

  async disconnect(): Promise<void> {
    await this.producer?.disconnect();
    this.producer = null;
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
    });
    byTopic.set(m.topic, arr);
  }
  return [...byTopic.entries()].map(([topic, msgs]) => ({
    topic,
    messages: msgs,
    ...(compression && compression !== "none" ? { compression } : {}),
  }));
}

async function importKafkaJs(): Promise<{ Kafka: new (cfg: unknown) => KjsKafka }> {
  try {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return (await import("kafkajs")) as any;
  } catch {
    throw new Error(
      'Driver "kafkajs" selected but the "kafkajs" package is not installed. Run: npm i kafkajs',
    );
  }
}
