---
"@eventferry/schema-registry": minor
---

Typed authentication for the Schema Registry HTTP API. New `auth?: SchemaRegistryAuth` option on `SchemaRegistrySerializer` with two shapes:

- `{ type: "basic", username, password }` — HTTP Basic, forwarded straight to the underlying client's `auth` config (the conventional Confluent Cloud + commercial-registry shape).
- `{ type: "bearer", token: string | () => string | Promise<string> }` — adds an `Authorization: Bearer <token>` header via a small middleware on the upstream client. Callable tokens are resolved on **every** request, so rotation logic lives in the caller's provider (cache inside your callable if rotation cost matters).

Ignored when an already-constructed `registry` client is injected — configure auth there yourself. mTLS to the registry stays out of scope: it's handled by a custom `https.Agent` on a self-constructed client (registry TLS is independent of broker TLS, and we don't want to fold an unrelated knob into this surface).

The middleware factory is exported as `bearerAuthMiddleware` for testing only — not part of the supported public API and may move in a future version.
