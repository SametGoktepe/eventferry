import type {
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
}

/**
 * The Publisher the Relay talks to. Wraps a pluggable KafkaDriver and
 * adds dead-letter routing. Works against Kafka and Redpanda identically
 * (Redpanda is Kafka-API compatible).
 */
export class KafkaPublisher implements Publisher {
  private readonly driver: KafkaDriver;

  constructor(opts: KafkaPublisherOptions) {
    this.driver = opts.customDriver ?? selectDriver(opts);
  }

  connect(): Promise<void> {
    return this.driver.connect();
  }

  disconnect(): Promise<void> {
    return this.driver.disconnect();
  }

  publish(messages: PublishableMessage[]): Promise<PublishResult[]> {
    return this.driver.sendBatch(messages);
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
