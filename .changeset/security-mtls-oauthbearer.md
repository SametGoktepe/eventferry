---
"@eventferry/kafka": minor
---

**feat: mTLS + SASL/OAUTHBEARER support**

Two new authentication paths for managed and enterprise Kafka clusters.

### mTLS (mutual TLS)

The `ssl` option now accepts a full `TlsConfig` in addition to the boolean shorthand:

```ts
new KafkaPublisher({
  brokers: ["broker:9093"],
  ssl: {
    ca:   readFileSync("/etc/ssl/kafka-ca.pem"),
    cert: readFileSync("/etc/ssl/client.pem"),
    key:  readFileSync("/etc/ssl/client-key.pem"),
    passphrase: "optional",
    servername: "broker.example.com", // SNI override
  },
});
```

Buffer and PEM-string inputs are both supported. `ssl: true` continues to work unchanged (one-way TLS using the driver's default trust store).

> `rejectUnauthorized` is intentionally NOT exposed. TLS verification is non-negotiable; pass the cluster CA via `ca` for dev clusters with self-signed certs.

### SASL/OAUTHBEARER

Required for Azure Event Hubs, Confluent Cloud with OAuth/SSO, and any OIDC-fronted cluster. Bring your own token provider:

```ts
new KafkaPublisher({
  brokers: ["broker:9093"],
  ssl: true,
  sasl: {
    mechanism: "oauthbearer",
    oauthBearerProvider: async () => ({
      value: bearerToken,
      principal: "user@realm",  // required on confluent
      lifetime: 3600_000,        // ms — required on confluent
      extensions: { scope: "read,write" },
    }),
  },
});
```

**Driver asymmetry to know about:** `kafkajs` reads only `value`; `@confluentinc/kafka-javascript` requires `value` + `principal` + `lifetime` (ms) and accepts an optional `extensions` map. Cross-driver portable providers should populate all four.

### Confluent driver internals

`@confluentinc/kafka-javascript` integrates via a small translator: simple `ssl: true` and SASL configs go through the kafkajs-compat layer, but a custom `TlsConfig` is mapped to the librdkafka PEM keys (`ssl.ca.pem`, `ssl.certificate.pem`, `ssl.key.pem`, `ssl.key.password`) and `security.protocol` is auto-derived (`ssl` / `sasl_plaintext` / `sasl_ssl`). Buffer inputs are coerced to UTF-8 strings (librdkafka does not accept Buffers).

### Backward compatibility

Pure-additive. Existing configs (`ssl: true | false | undefined`, password SASL) work unchanged.
