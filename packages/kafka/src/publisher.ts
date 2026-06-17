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

  constructor(opts: KafkaPublisherOptions) {
    this.logger = opts.logger;
    this.hooks = opts.hooks ?? {};
    this.tracer = opts.tracer ?? new NoopKafkaTracer();
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
    await safeHook(this.logger, "onConnect", () => this.hooks.onConnect?.());
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
    let results: PublishResult[];
    try {
      results = await this.driver.sendBatch(messages);
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
