import type {
  Logger,
  PublishableMessage,
  Publisher,
  PublishResult,
} from "@eventferry/core";
import { ConfluentDriver } from "./confluent-driver.js";
import type {
  DriverKind,
  KafkaConnectionConfig,
  KafkaDriver,
  ProducerBehaviorConfig,
} from "./driver.js";
import { KafkaJsDriver } from "./kafkajs-driver.js";
import { safeHook } from "./hooks.js";
import type { KafkaPublisherHooks } from "./hooks.js";
import { NoopKafkaTracer } from "./tracing.js";
import type { KafkaTracer, SpanLike } from "./tracing.js";
import type {
  KafkaAdmin,
  KafkaDriverAdmin,
  PartitionGrowSpec,
  TopicCreateSpec,
} from "./admin.js";

export interface KafkaPublisherOptions
  extends KafkaConnectionConfig,
    ProducerBehaviorConfig {
  /** Which underlying client to use. Default "kafkajs". */
  driver?: DriverKind;
  /**
   * Provide a fully custom driver instance instead of the built-ins.
   * Useful for testing or unsupported clients.
   */
  customDriver?: KafkaDriver;
  /**
   * Optional structured logger. When set, the publisher routes its own
   * diagnostics (driver warnings, hook failures) through it. When omitted,
   * the publisher is silent — only the underlying drivers may still log.
   */
  logger?: Logger;
  /**
   * Optional lifecycle hooks. Every hook is invoked safely (try/catch +
   * logged via `logger`) and a misbehaving hook will never break publishing.
   */
  hooks?: KafkaPublisherHooks;
  /**
   * Optional tracer. When set, `publish()` wraps each batch in a span that
   * follows the current stable OpenTelemetry messaging semantic conventions.
   * Use a thin adapter over your tracing SDK (see {@link KafkaTracer}).
   */
  tracer?: KafkaTracer;
  /**
   * If set, `connect()` checks that every topic in this list exists on the
   * cluster and throws a descriptive error if any are missing. Use this to
   * fail-fast at startup instead of letting the first send-time error
   * surprise you.
   *
   * Validation runs AFTER the producer connects but BEFORE `onConnect` hooks
   * fire. Driver must implement `admin()` (the built-ins do).
   */
  validateTopicsOnConnect?: string[];
  /**
   * Transparently recover from a producer-fence error. When set to `true`,
   * a `publish()` call whose batch comes back with at least one
   * `errorKind: "fenced"` result triggers ONE round of:
   *
   *   1. disconnect the driver
   *   2. connect it again (re-running `initTransactions` for transactional producers)
   *   3. re-send the same batch
   *
   * If the second send still produces a fenced result, the publisher gives
   * up and surfaces the failures unchanged — at that point the fence is
   * almost certainly caused by another instance taking the same
   * `transactionalId`, and silently retrying again would mask the
   * misconfiguration.
   *
   * Default `false` to preserve the previous "fenced → fatal" semantics.
   * Turn it on when running a single producer instance against transient
   * brokers (rolling restarts, network blips) where a fence is usually
   * just a transient epoch mismatch.
   *
   * For MULTI-INSTANCE EOS, leave this OFF and use a callable
   * `transactionalId` derived from per-instance context (pod name, k8s
   * ordinal, AZ + replica index) so each instance has a stable, unique
   * id — fences will then correctly stop the loser instance.
   */
  autoRecoverFromFence?: boolean;
}

/**
 * The Publisher the Relay talks to. Wraps a pluggable KafkaDriver and adds
 * dead-letter routing, observability hooks, and OpenTelemetry-shaped publish
 * spans. Works against Kafka and Redpanda identically (Redpanda is Kafka-API
 * compatible).
 */
export class KafkaPublisher implements Publisher {
  private readonly driver: KafkaDriver;
  private readonly logger: Logger | undefined;
  private readonly hooks: KafkaPublisherHooks;
  private readonly tracer: KafkaTracer;
  private readonly validateTopicsOnConnect: readonly string[] | undefined;
  private readonly autoRecoverFromFence: boolean;
  // Serialize reconnects so concurrent publish() calls hitting a fence
  // all observe the same single reconnect attempt — the second publish
  // doesn't try to disconnect a producer the first is still re-initing.
  private fenceRecovery: Promise<void> | null = null;

  constructor(opts: KafkaPublisherOptions) {
    this.logger = opts.logger;
    this.hooks = opts.hooks ?? {};
    this.tracer = opts.tracer ?? new NoopKafkaTracer();
    this.validateTopicsOnConnect = opts.validateTopicsOnConnect
      ? Object.freeze([...opts.validateTopicsOnConnect])
      : undefined;
    this.autoRecoverFromFence = opts.autoRecoverFromFence ?? false;
    // Plumb the logger into driver construction so driver-side diagnostics
    // (e.g. kafkajs unsupported-tuning warnings) route through it too.
    // Plumb a safe-wrapped onTransactionAbort callback so the driver-level
    // transaction abort path fans out to the user-supplied hook safely.
    const onTransactionAbort = this.hooks.onTransactionAbort
      ? (error: Error) => {
          void safeHook(this.logger, "onTransactionAbort", () =>
            this.hooks.onTransactionAbort?.(error),
          );
        }
      : undefined;
    this.driver =
      opts.customDriver ?? selectDriver({ ...opts, onTransactionAbort });
  }

  async connect(): Promise<void> {
    await this.driver.connect();
    if (this.validateTopicsOnConnect && this.validateTopicsOnConnect.length) {
      await this.assertTopicsExist(this.validateTopicsOnConnect);
    }
    await safeHook(this.logger, "onConnect", () => this.hooks.onConnect?.());
  }

  /**
   * Borrow a new admin client from the driver. The returned admin is
   * connected and ready to use; the CALLER must `close()` it. Throws if the
   * driver does not implement admin (custom driver lacking the capability).
   */
  async admin(): Promise<KafkaAdmin> {
    const driverAdmin = await this.openDriverAdmin();
    return driverAdmin;
  }

  /**
   * Idempotently provision topics. Each spec creates the topic if absent;
   * existing topics are skipped without error. If `growPartitions: true`
   * (default false), topics whose current partition count is below the
   * requested `numPartitions` are grown via `createPartitions`.
   *
   * Replication factor and config entries on EXISTING topics are NOT
   * reconciled — Kafka does not provide a safe in-place alter for those
   * (changing replication requires reassignment; configs use alterConfigs).
   * Reach for the raw admin if you need that.
   */
  async ensureTopics(
    specs: TopicCreateSpec[],
    opts: { growPartitions?: boolean } = {},
  ): Promise<void> {
    if (specs.length === 0) return;
    const admin = await this.openDriverAdmin();
    try {
      const topicNames = specs.map((s) => s.topic);
      const existing = await admin.describeTopics(topicNames);
      const existingByName = new Map(existing.map((t) => [t.topic, t]));

      const toCreate = specs.filter(
        (s) => (existingByName.get(s.topic)?.partitions.length ?? 0) === 0,
      );
      if (toCreate.length) await admin.createTopics(toCreate);

      if (opts.growPartitions) {
        const grow: PartitionGrowSpec[] = [];
        for (const s of specs) {
          if (s.numPartitions === undefined) continue;
          const current = existingByName.get(s.topic);
          const currentCount = current?.partitions.length ?? 0;
          if (currentCount > 0 && currentCount < s.numPartitions) {
            grow.push({ topic: s.topic, totalCount: s.numPartitions });
          }
        }
        if (grow.length) await admin.createPartitions(grow);
      }
    } finally {
      await admin.close();
    }
  }

  /**
   * Borrow a fresh admin from the driver and connect it. Throws when the
   * driver does not implement admin (custom drivers without that capability).
   */
  private async openDriverAdmin(): Promise<KafkaDriverAdmin> {
    if (!this.driver.admin) {
      throw new Error(
        "KafkaPublisher: configured driver does not implement admin(). " +
          "Use the built-in kafkajs or confluent driver, or extend your custom driver.",
      );
    }
    const admin = await this.driver.admin();
    await admin.connect();
    return admin;
  }

  /**
   * Open an admin, list topics, throw if any required topic is missing.
   * Always closes the admin (success or failure).
   */
  private async assertTopicsExist(required: readonly string[]): Promise<void> {
    const admin = await this.openDriverAdmin();
    try {
      const all = new Set(await admin.listTopics());
      const missing = required.filter((t) => !all.has(t));
      if (missing.length) {
        throw new Error(
          `KafkaPublisher: validateTopicsOnConnect failed — topics missing on cluster: ${missing.join(", ")}`,
        );
      }
    } finally {
      await admin.close();
    }
  }

  async disconnect(): Promise<void> {
    await this.driver.disconnect();
    await safeHook(this.logger, "onDisconnect", () =>
      this.hooks.onDisconnect?.(),
    );
  }

  async publish(messages: PublishableMessage[]): Promise<PublishResult[]> {
    if (messages.length === 0) return [];

    const span = this.startBatchSpan(messages);
    // If the tracer can inject trace context (W3C `traceparent`/`tracestate`
    // is the common case), clone each message and let the tracer enrich its
    // headers. We MUST NOT mutate the caller's PublishableMessage objects —
    // the relay reuses the same record reference across retries and any
    // mutation would corrupt later attempts.
    const outgoing: PublishableMessage[] = this.tracer.inject
      ? messages.map((m) => {
          const headers = { ...m.headers };
          this.tracer.inject!(span, headers);
          return { ...m, headers };
        })
      : messages;
    let results: PublishResult[];
    try {
      results = await this.driver.sendBatch(outgoing);
    } catch (err) {
      // Driver-level throw — every record is a failure attributed to the
      // batch-level error. Record on the span, fire hook, rethrow.
      const error = err instanceof Error ? err : new Error(String(err));
      span.setStatus({ code: "error", message: error.message });
      span.recordException(error);
      span.end();
      await safeHook(this.logger, "onError", () => this.hooks.onError?.(error));
      throw err;
    }

    // Fence detection + transparent single-shot recovery. Runs BEFORE the
    // per-record hooks so observers see a clean "all ok" path when the
    // retry succeeds — they only see the fence error if the second attempt
    // also fails. Fires the onProducerFenced hook regardless of whether
    // auto-recovery is enabled (informational signal).
    const firstFenced = results.find(
      (r) => !r.ok && r.errorKind === "fenced",
    );
    if (firstFenced) {
      const fenceErr = firstFenced.error ?? new Error("producer fenced");
      await safeHook(this.logger, "onProducerFenced", () =>
        this.hooks.onProducerFenced?.(fenceErr),
      );
      if (this.autoRecoverFromFence) {
        results = await this.recoverAndRetry(outgoing, results);
      }
    }

    // Per-record hooks. Walk by index so the original message is available.
    const byId = new Map(messages.map((m) => [m.recordId, m]));
    let allOk = true;
    for (const r of results) {
      const msg = byId.get(r.recordId);
      if (!msg) continue;
      await safeHook(this.logger, "onPublish", () =>
        this.hooks.onPublish?.(r, msg),
      );
      if (!r.ok) {
        allOk = false;
        const err = r.error ?? new Error("publish failed");
        await safeHook(this.logger, "onError", () =>
          this.hooks.onError?.(err, msg),
        );
      }
    }

    span.setStatus(allOk ? { code: "ok" } : { code: "error" });
    span.end();
    return results;
  }

  /**
   * Send a single dead-lettered message. The message already carries the
   * DLQ topic (the Relay rewrites it), plus the failure reason as a header.
   */
  async publishToDlq(message: PublishableMessage, error: Error): Promise<void> {
    const dlqMessage: PublishableMessage = {
      ...message,
      headers: {
        ...message.headers,
        "dlq-reason": error.message,
        // Error class name (e.g. "KafkaJSProtocolError", "RecordTooLargeException"),
        // useful for downstream alert routing without parsing the reason string.
        "dlq-error-class": error.name || error.constructor?.name || "Error",
        "dlq-original-topic": message.headers["original-topic"] ?? "",
        "dlq-failed-at": new Date().toISOString(),
      },
    };
    const [result] = await this.driver.sendBatch([dlqMessage]);
    if (result && !result.ok) {
      throw result.error ?? new Error("DLQ publish failed");
    }
  }

  /** Whether the configured driver provides atomic (EOS) batch sends. */
  get transactional(): boolean {
    return this.driver.transactional;
  }

  /**
   * Disconnect + re-connect the driver and re-send the batch ONCE. Used
   * by the fence-recovery path. Concurrent fence recoveries dedupe on a
   * shared in-flight promise (`fenceRecovery`) so we don't tear the
   * producer down while another batch is mid-restart.
   *
   * If the second send STILL reports any fenced records, those failures
   * are returned unchanged — another instance has almost certainly taken
   * the same `transactionalId` and silently retrying again would mask
   * the misconfiguration.
   */
  private async recoverAndRetry(
    outgoing: PublishableMessage[],
    firstResults: PublishResult[],
  ): Promise<PublishResult[]> {
    if (!this.fenceRecovery) {
      this.fenceRecovery = (async () => {
        try {
          await this.driver.disconnect();
          await this.driver.connect();
        } finally {
          // Clear the slot so a SUBSEQUENT fence can attempt recovery
          // again — recoveries are per-incident, not per-process.
          this.fenceRecovery = null;
        }
      })();
    }
    try {
      await this.fenceRecovery;
    } catch (err) {
      // Reconnect itself failed — surface the original fence result so
      // the relay can DLQ + alert. The reconnect error is informational.
      const reconnectErr =
        err instanceof Error ? err : new Error(String(err));
      await safeHook(this.logger, "onError", () =>
        this.hooks.onError?.(reconnectErr),
      );
      return firstResults;
    }
    try {
      return await this.driver.sendBatch(outgoing);
    } catch {
      // Driver threw on the second attempt — surface original fence results.
      return firstResults;
    }
  }

  /**
   * Start a span for the batch following the OTel messaging conventions.
   *
   * Multi-topic batches: per the OTel spec, the span name uses the
   * destination — we pick the FIRST topic in the batch and document the
   * limitation. Callers that publish heterogeneous batches and care about
   * per-topic spans should split their batches upstream.
   */
  private startBatchSpan(messages: PublishableMessage[]): SpanLike {
    const topic = messages[0]?.topic ?? "unknown";
    return this.tracer.startPublishSpan(`${topic} publish`, {
      "messaging.system": "kafka",
      "messaging.operation.type": "publish",
      "messaging.destination.name": topic,
      "messaging.batch.message_count": messages.length,
    });
  }
}

function selectDriver(opts: KafkaPublisherOptions): KafkaDriver {
  const kind = opts.driver ?? "kafkajs";
  switch (kind) {
    case "kafkajs":
      return new KafkaJsDriver(opts);
    case "confluent":
      return new ConfluentDriver(opts);
    default:
      throw new Error(`Unknown driver "${kind}"`);
  }
}
