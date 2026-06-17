---
"@eventferry/kafka": minor
---

**feat: callable `transactionalId` + abort-aware tx hook**

### Callable `transactionalId`

`transactionalId` accepts a sync or async resolver in addition to a plain string:

```ts
new KafkaPublisher({
  brokers,
  transactional: true,
  transactionalId: () =>
    `${process.env.POD_NAME}-${process.env.REPLICA_INDEX}`,
});
```

Useful when the id depends on runtime context that isn't known at construction time (pod name, AZ + replica index, k8s ordinal). For multi-instance EOS, the resolved id MUST be stable across a single instance's restarts but UNIQUE across instances. The plain-string form remains supported and unchanged.

### Abort-aware `onTransactionAbort` hook

When a transactional `sendBatch` triggers the abort path, the publisher fires `hooks.onTransactionAbort(err)` so dashboards and metrics catch EOS failure rates:

```ts
new KafkaPublisher({
  brokers,
  transactional: true,
  transactionalId: "orders-tx",
  hooks: {
    onTransactionAbort: (err) => metrics.txAborts.inc({ reason: err.name }),
  },
});
```

Best-effort: the hook is safe-wrapped (a throwing hook never breaks the abort path); both `kafkajs` and `@confluentinc/kafka-javascript` drivers fire it from their transaction catch blocks.

### Backward compatibility

100% additive. Existing call sites — string `transactionalId`, no hooks — work unchanged.
