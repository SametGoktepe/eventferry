---
"@eventferry/core": minor
"@eventferry/postgres": minor
---

Add W3C trace propagation (OpenTelemetry-compatible), dependency-free.

- **core:** new `Tracing` interface (`inject(carrier)`), the shape of an OpenTelemetry
  `TextMapPropagator` — the library depends on no tracing package.
- **postgres:** `PostgresStore({ tracing })` captures the active W3C
  `traceparent`/`tracestate` into the row's headers at `enqueue`, so it rides along to
  the published message (on every path — polling, notify, streaming — since headers
  already pass through) and the consumer can continue the trace.
- The caller's `headers` object is never mutated. With no `tracing` configured,
  behavior is unchanged. The existing `trace-id` header stays for simple correlation.
- OpenTelemetry/Datadog/custom integrate via a ~5-line adapter (documented, not bundled).
