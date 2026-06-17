---
"@eventferry/kafka": minor
---

**feat: OpenTelemetry publish span + hook surface + logger passthrough**

### OpenTelemetry tracing

`KafkaPublisher` now accepts an optional `tracer` that follows the current stable [OpenTelemetry messaging semantic conventions](https://github.com/open-telemetry/semantic-conventions/blob/main/docs/messaging/kafka.md). One span per `publish()` call, named `"{topic} publish"`, with `messaging.system=kafka`, `messaging.operation.type=publish`, `messaging.destination.name=<topic>`, and `messaging.batch.message_count=<n>`. No dependency on `@opentelemetry/api` — wire through a 10-line adapter:

```ts
import { trace, SpanKind, SpanStatusCode } from "@opentelemetry/api";
import type { KafkaTracer } from "@eventferry/kafka";

const otel = trace.getTracer("@eventferry/kafka");
const tracer: KafkaTracer = {
  startPublishSpan(name, attributes) {
    const span = otel.startSpan(name, { kind: SpanKind.PRODUCER, attributes });
    return { /* setAttribute, setStatus, recordException, end */ };
  },
};

new KafkaPublisher({ brokers, tracer });
```

### Hook surface

`KafkaPublisher` now accepts `hooks` for observability and metrics integration:

```ts
new KafkaPublisher({
  brokers,
  hooks: {
    onConnect, onDisconnect,
    onPublish, onError, onTransactionAbort,
  },
});
```

Hooks are **safe by construction**: a throwing hook never breaks publishing — the publisher catches and logs via the configured `logger`.

### Logger passthrough

A new optional `logger?: Logger` field on `KafkaPublisherOptions` (same `Logger` interface as `@eventferry/core`). Routes the publisher's own diagnostics (driver warnings about unsupported tuning, hook failures) through your logging stack instead of `console.warn`. When omitted, behavior matches today (drivers still fall back to `console.warn`).

### Backward compatibility

100% additive. Existing call sites (no hooks, no tracer, no logger) work unchanged — the tracer defaults to a `NoopKafkaTracer`, the hook map defaults to `{}`, and the logger stays undefined.
