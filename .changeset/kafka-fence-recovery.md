---
"@eventferry/core": minor
"@eventferry/kafka": minor
---

Producer-fenced restart. `PRODUCER_FENCED` and `INVALID_PRODUCER_EPOCH` errors now classify as `errorKind: "fenced"` (previously bundled into `"fatal"`). The new kind is documented as **transient by default** — fences also fire on broker restart and network partition recovery, not only on multi-instance collisions.

New publisher option `autoRecoverFromFence: boolean` (default `false`): when on, a publish batch reporting at least one fenced result triggers exactly one `disconnect → connect → re-send same batch` cycle. Transactional producers re-run `initTransactions` as part of the reconnect. If the second send still reports any fenced record, the publisher gives up — silently retrying again would mask a real misconfiguration. Concurrent fenced publishes share a single in-flight reconnect so the producer is not torn down twice mid-restart.

New `KafkaPublisherHooks.onProducerFenced(error)` hook fires regardless of the recovery flag — informational signal so dashboards can track fence rates whether or not the publisher attempts recovery.

`@eventferry/core` minor: `PublishErrorKind` union gains `"fenced"`. The relay treats unknown / `"retriable"` / `"fenced"` identically (retry per backoff, DLQ on `attempts > maxAttempts`) — no relay-level changes required, but the new kind shows up in logs and the `errorKind` field of `PublishResult`.

Multi-instance EOS guidance: leave `autoRecoverFromFence` OFF and use a callable `transactionalId` that derives a stable, unique id per instance (pod name + replica index). Cross-instance fence is the broker telling the loser instance to stop — recovering silently creates a thrashing leadership flip. The README now spells this out in a `Producer-fenced restart` section.
