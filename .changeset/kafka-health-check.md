---
"@eventferry/kafka": minor
---

`publisher.healthCheck({ timeoutMs })` — cheap reachability probe usable as the body of `/healthz` or `/readyz`. Borrows a fresh admin client, calls `listTopics`, and returns a stable `HealthStatus` shape: `{ ok, latencyMs, timestamp, error? }`. Default timeout 5000 ms (long enough to ride out a single broker leader election, short enough to fail a liveness probe meaningfully); `timeoutMs: 0` disables the timer entirely.

What it proves: the broker is reachable AND the configured credentials still authenticate. What it does NOT prove: the producer's send path is fully operational — a fenced transactional producer would still answer healthy here. Documented as "broker reachable + auth still good", not "publisher fully operational".

The borrowed admin is always closed (success, failure, timeout — try/finally). Admin-side close failures are swallowed; health checks aren't the place to crash. Custom drivers without an `admin()` method return `{ ok: false, error: ... }` instead of the throw `publisher.admin()` would surface.
