/**
 * Tracing surface for the publisher.
 *
 * eventferry deliberately does not depend on `@opentelemetry/api` — instead
 * users wire a thin adapter over their tracing system (OpenTelemetry,
 * Datadog, internal, …). This file defines the minimal contract; an
 * OpenTelemetry adapter is ~10 lines (see the README).
 *
 * The contract follows the **current stable** OpenTelemetry messaging
 * semantic conventions
 * ({@link https://github.com/open-telemetry/semantic-conventions/blob/main/docs/messaging/kafka.md spec}):
 *
 *   - Span name: `"{topic} publish"`
 *   - `SpanKind.PRODUCER`
 *   - Required attributes: `messaging.system=kafka`,
 *     `messaging.operation.type=publish`, `messaging.destination.name=<topic>`
 *   - Recommended: `messaging.batch.message_count`, `messaging.kafka.partition`,
 *     `server.address`, `server.port`
 *   - One span per batch (NOT per message — per-message spans cause
 *     cardinality explosion and the spec actively warns against this)
 */

/** Attribute values the spec allows. */
export type SpanAttributeValue = string | number | boolean;

/**
 * Minimal span surface the publisher needs. Implementations wrap a
 * tracing-system-specific span; methods MUST never throw out of the
 * publisher's hot path (wrap your own SDK calls in try/catch).
 */
export interface SpanLike {
  setAttribute(key: string, value: SpanAttributeValue): void;
  setAttributes(attrs: Record<string, SpanAttributeValue>): void;
  /** OK on success; ERROR on failure. The `message` is the error message. */
  setStatus(status: { code: "ok" | "error"; message?: string }): void;
  /** Attach an exception to the span (OpenTelemetry `recordException`). */
  recordException(error: Error): void;
  end(): void;
}

/**
 * Factory the publisher calls once per `sendBatch` to start a span.
 * Implementations MUST set `SpanKind.PRODUCER` and the messaging semconv
 * attributes on the returned span before returning it.
 */
export interface KafkaTracer {
  /**
   * Start a publish span.
   * @param name        Recommended format: `"{topic} publish"`.
   * @param attributes  Initial attributes (the publisher supplies the messaging
   *                    semconv set: system, destination.name, operation.type,
   *                    batch.message_count, plus optional kafka.partition and
   *                    server.address/port).
   */
  startPublishSpan(
    name: string,
    attributes: Record<string, SpanAttributeValue>,
  ): SpanLike;

  /**
   * OPTIONAL: inject the active trace context (W3C `traceparent` +
   * `tracestate`) into a per-message header map. Called by the publisher
   * AFTER the batch span is created and BEFORE the records hit the wire.
   *
   * Implementations typically wrap OpenTelemetry's `propagation.inject(...)`
   * or your tracing SDK's equivalent. Mutate the `headers` object in
   * place — the publisher allocates a fresh copy per message so this is
   * safe and matches the propagation API of every major SDK.
   *
   * Tracers without distributed-context propagation (or that only care
   * about local spans) may leave this off — consumers can still derive
   * trace headers themselves by other means.
   */
  inject?(span: SpanLike, headers: Record<string, string>): void;
}

/**
 * No-op tracer. Used when the user does not configure one. Cheap allocation
 * — never touches I/O.
 */
export class NoopKafkaTracer implements KafkaTracer {
  startPublishSpan(): SpanLike {
    return NOOP_SPAN;
  }
}

const NOOP_SPAN: SpanLike = {
  setAttribute() {},
  setAttributes() {},
  setStatus() {},
  recordException() {},
  end() {},
};
