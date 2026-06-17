---
"@eventferry/kafka": minor
---

Add consumer-side helpers via the new `@eventferry/kafka/consume` subpath import: `decode(message, { decoder })` normalizes the raw message shape (key, value, headers, offset, timestamp, partition) both kafkajs and confluent deliver — with built-in `json` / `utf8` / `none` decoders plus a custom-function escape hatch; `extractTraceContext(headers)` parses the W3C `traceparent` / `tracestate` headers (strict validation per the W3C Trace Context spec) and accepts both raw (Buffer) and decoded (string) header shapes. Paired on the producer side with a new optional `KafkaTracer.inject(span, headers)` hook so OpenTelemetry users can complete the publish→consume trace propagation in two lines. The publisher clones each message before invoking `inject` — the caller's `PublishableMessage` references are never mutated, keeping the relay's retry path safe.
