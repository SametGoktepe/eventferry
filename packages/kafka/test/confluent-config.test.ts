import { describe, expect, it } from "vitest";
import { buildConfluentClientConfig } from "../src/confluent-config.js";

describe("buildConfluentClientConfig — base shape", () => {
  it("passes brokers + clientId through to the kafkaJS surface", () => {
    const { kafkaJS, librdkafka } = buildConfluentClientConfig({
      brokers: ["b1:9092", "b2:9092"],
      clientId: "my-app",
    });
    expect(kafkaJS["brokers"]).toEqual(["b1:9092", "b2:9092"]);
    expect(kafkaJS["clientId"]).toBe("my-app");
    expect(librdkafka).toEqual({});
  });

  it("defaults clientId to 'eventferry' when omitted", () => {
    const { kafkaJS } = buildConfluentClientConfig({ brokers: ["b:9092"] });
    expect(kafkaJS["clientId"]).toBe("eventferry");
  });

  it("leaves security.protocol unset when neither TLS nor SASL is configured", () => {
    const { librdkafka } = buildConfluentClientConfig({ brokers: ["b:9092"] });
    expect(librdkafka["security.protocol"]).toBeUndefined();
  });
});

describe("buildConfluentClientConfig — simple TLS boolean", () => {
  it("ssl: true keeps the kafkaJS-compat boolean and sets security.protocol=ssl", () => {
    const { kafkaJS, librdkafka } = buildConfluentClientConfig({
      brokers: ["b:9092"],
      ssl: true,
    });
    expect(kafkaJS["ssl"]).toBe(true);
    expect(librdkafka["security.protocol"]).toBe("ssl");
    // No PEM keys when only the simple boolean is requested.
    expect(librdkafka["ssl.ca.pem"]).toBeUndefined();
    expect(librdkafka["ssl.certificate.pem"]).toBeUndefined();
  });
});

describe("buildConfluentClientConfig — mTLS (TlsConfig)", () => {
  it("translates string PEMs to librdkafka ssl.*.pem keys", () => {
    const { kafkaJS, librdkafka } = buildConfluentClientConfig({
      brokers: ["b:9092"],
      ssl: {
        ca: "-----BEGIN CERTIFICATE-----\nCA\n-----END CERTIFICATE-----",
        cert: "-----BEGIN CERTIFICATE-----\nCERT\n-----END CERTIFICATE-----",
        key: "-----BEGIN PRIVATE KEY-----\nKEY\n-----END PRIVATE KEY-----",
        passphrase: "s3cret",
      },
    });
    expect(librdkafka["security.protocol"]).toBe("ssl");
    expect(librdkafka["ssl.ca.pem"]).toContain("BEGIN CERTIFICATE");
    expect(librdkafka["ssl.certificate.pem"]).toContain("CERT");
    expect(librdkafka["ssl.key.pem"]).toContain("KEY");
    expect(librdkafka["ssl.key.password"]).toBe("s3cret");
    // Avoid double-sending: when TlsConfig is provided, the kafkaJS-compat
    // ssl bool should NOT be set (otherwise both layers fight).
    expect(kafkaJS["ssl"]).toBeUndefined();
  });

  it("coerces Buffer PEMs to UTF-8 strings (librdkafka does not accept Buffer)", () => {
    const caBuf = Buffer.from("CA-BYTES", "utf8");
    const certBuf = Buffer.from("CERT-BYTES", "utf8");
    const { librdkafka } = buildConfluentClientConfig({
      brokers: ["b:9092"],
      ssl: { ca: caBuf, cert: certBuf, key: "PEMSTRING" },
    });
    expect(librdkafka["ssl.ca.pem"]).toBe("CA-BYTES");
    expect(librdkafka["ssl.certificate.pem"]).toBe("CERT-BYTES");
    expect(librdkafka["ssl.key.pem"]).toBe("PEMSTRING");
  });

  it("joins an array of CAs with newlines", () => {
    const { librdkafka } = buildConfluentClientConfig({
      brokers: ["b:9092"],
      ssl: { ca: ["CA-1", Buffer.from("CA-2", "utf8")] },
    });
    expect(librdkafka["ssl.ca.pem"]).toBe("CA-1\nCA-2");
  });
});

describe("buildConfluentClientConfig — SASL", () => {
  it("PLAIN: passes through kafkaJS-compat and sets sasl_plaintext", () => {
    const { kafkaJS, librdkafka } = buildConfluentClientConfig({
      brokers: ["b:9092"],
      sasl: { mechanism: "plain", username: "u", password: "p" },
    });
    expect(kafkaJS["sasl"]).toEqual({
      mechanism: "plain",
      username: "u",
      password: "p",
    });
    expect(librdkafka["security.protocol"]).toBe("sasl_plaintext");
  });

  it("SCRAM-SHA-512 + ssl: true → sasl_ssl", () => {
    const { librdkafka } = buildConfluentClientConfig({
      brokers: ["b:9092"],
      ssl: true,
      sasl: { mechanism: "scram-sha-512", username: "u", password: "p" },
    });
    expect(librdkafka["security.protocol"]).toBe("sasl_ssl");
  });

  it("OAUTHBEARER: provider callback is passed through under sasl", () => {
    const provider = async () => ({
      value: "tok",
      principal: "user@realm",
      lifetime: 3600_000,
      extensions: { scope: "read,write" },
    });
    const { kafkaJS, librdkafka } = buildConfluentClientConfig({
      brokers: ["b:9092"],
      ssl: true,
      sasl: { mechanism: "oauthbearer", oauthBearerProvider: provider },
    });
    expect((kafkaJS["sasl"] as { mechanism: string }).mechanism).toBe(
      "oauthbearer",
    );
    expect(
      (kafkaJS["sasl"] as { oauthBearerProvider: () => unknown }).oauthBearerProvider,
    ).toBe(provider);
    expect(librdkafka["security.protocol"]).toBe("sasl_ssl");
  });

  it("OAUTHBEARER + mTLS together set sasl_ssl and surface the PEM keys", () => {
    const { librdkafka } = buildConfluentClientConfig({
      brokers: ["b:9092"],
      ssl: { ca: "CA", cert: "CERT", key: "KEY" },
      sasl: {
        mechanism: "oauthbearer",
        oauthBearerProvider: async () => ({
          value: "tok",
          principal: "p",
          lifetime: 60_000,
        }),
      },
    });
    expect(librdkafka["security.protocol"]).toBe("sasl_ssl");
    expect(librdkafka["ssl.ca.pem"]).toBe("CA");
    expect(librdkafka["ssl.certificate.pem"]).toBe("CERT");
    expect(librdkafka["ssl.key.pem"]).toBe("KEY");
  });
});

describe("buildConfluentClientConfig — producer tuning passthrough", () => {
  it("maps each tuning knob to its librdkafka config key", () => {
    const { librdkafka } = buildConfluentClientConfig({
      brokers: ["b:9092"],
      lingerMs: 25,
      batchSize: 131_072,
      maxInFlightRequests: 5,
      requestTimeoutMs: 30_000,
      deliveryTimeoutMs: 120_000,
      maxRequestSize: 2_000_000,
      transactionTimeoutMs: 90_000,
    });
    expect(librdkafka["linger.ms"]).toBe(25);
    expect(librdkafka["batch.size"]).toBe(131_072);
    expect(librdkafka["max.in.flight.requests.per.connection"]).toBe(5);
    expect(librdkafka["request.timeout.ms"]).toBe(30_000);
    expect(librdkafka["delivery.timeout.ms"]).toBe(120_000);
    expect(librdkafka["message.max.bytes"]).toBe(2_000_000);
    expect(librdkafka["transaction.timeout.ms"]).toBe(90_000);
  });

  it("omits tuning keys when the user doesn't set them", () => {
    const { librdkafka } = buildConfluentClientConfig({ brokers: ["b:9092"] });
    expect(librdkafka["linger.ms"]).toBeUndefined();
    expect(librdkafka["batch.size"]).toBeUndefined();
    expect(librdkafka["request.timeout.ms"]).toBeUndefined();
    expect(librdkafka["delivery.timeout.ms"]).toBeUndefined();
    expect(librdkafka["message.max.bytes"]).toBeUndefined();
    expect(librdkafka["transaction.timeout.ms"]).toBeUndefined();
  });

  it("honors a zero value (0 is a valid lingerMs setting)", () => {
    const { librdkafka } = buildConfluentClientConfig({
      brokers: ["b:9092"],
      lingerMs: 0,
    });
    expect(librdkafka["linger.ms"]).toBe(0);
  });

  it("compressionLevel maps to librdkafka compression.level", () => {
    const { librdkafka } = buildConfluentClientConfig({
      brokers: ["b:9092"],
      compression: "zstd",
      compressionLevel: 9,
    });
    expect(librdkafka["compression.level"]).toBe(9);
  });
});

describe("buildConfluentClientConfig — rawProducerConfig escape hatch", () => {
  it("merges raw keys into librdkafka config", () => {
    const { librdkafka } = buildConfluentClientConfig({
      brokers: ["b:9092"],
      rawProducerConfig: {
        "queue.buffering.max.messages": 100_000,
        "statistics.interval.ms": 5000,
      },
    });
    expect(librdkafka["queue.buffering.max.messages"]).toBe(100_000);
    expect(librdkafka["statistics.interval.ms"]).toBe(5000);
  });

  it("raw keys WIN against translated ones (escape-hatch precedence)", () => {
    const { librdkafka } = buildConfluentClientConfig({
      brokers: ["b:9092"],
      lingerMs: 10,
      rawProducerConfig: { "linger.ms": 50 },
    });
    expect(librdkafka["linger.ms"]).toBe(50);
  });

  it("absent rawProducerConfig leaves librdkafka config untouched", () => {
    const { librdkafka } = buildConfluentClientConfig({ brokers: ["b:9092"] });
    expect(librdkafka).toEqual({});
  });
});

describe("buildConfluentClientConfig — librdkafka stats hook (Phase C1)", () => {
  it("sets stats_cb + default statistics.interval.ms=30000 when onStats is provided", () => {
    const { librdkafka } = buildConfluentClientConfig({
      brokers: ["b:9092"],
      onStats: () => {},
    });
    expect(typeof librdkafka["stats_cb"]).toBe("function");
    expect(librdkafka["statistics.interval.ms"]).toBe(30_000);
  });

  it("honors statsIntervalMs override when onStats is set", () => {
    const { librdkafka } = buildConfluentClientConfig({
      brokers: ["b:9092"],
      onStats: () => {},
      statsIntervalMs: 5_000,
    });
    expect(librdkafka["statistics.interval.ms"]).toBe(5_000);
  });

  it("does NOT enable the stats timer when onStats is absent (CPU-billed)", () => {
    const { librdkafka } = buildConfluentClientConfig({ brokers: ["b:9092"] });
    expect(librdkafka["stats_cb"]).toBeUndefined();
    expect(librdkafka["statistics.interval.ms"]).toBeUndefined();
  });

  it("statsIntervalMs without onStats still sets the interval (raw client listener escape)", () => {
    const { librdkafka } = buildConfluentClientConfig({
      brokers: ["b:9092"],
      statsIntervalMs: 10_000,
    });
    expect(librdkafka["stats_cb"]).toBeUndefined();
    expect(librdkafka["statistics.interval.ms"]).toBe(10_000);
  });

  it("rawProducerConfig WINS against the default interval (escape-hatch precedence preserved)", () => {
    const { librdkafka } = buildConfluentClientConfig({
      brokers: ["b:9092"],
      onStats: () => {},
      rawProducerConfig: { "statistics.interval.ms": 1_000 },
    });
    expect(librdkafka["statistics.interval.ms"]).toBe(1_000);
  });

  it("stats_cb parses the JSON string librdkafka emits and forwards a plain object", () => {
    const seen: unknown[] = [];
    const { librdkafka } = buildConfluentClientConfig({
      brokers: ["b:9092"],
      onStats: (stats) => seen.push(stats),
    });
    const cb = librdkafka["stats_cb"] as (raw: string) => void;
    cb('{"name":"prod-1","txmsgs":42}');
    expect(seen[0]).toEqual({ name: "prod-1", txmsgs: 42 });
  });

  it("stats_cb forwards an already-parsed object untouched (defensive)", () => {
    const seen: unknown[] = [];
    const { librdkafka } = buildConfluentClientConfig({
      brokers: ["b:9092"],
      onStats: (stats) => seen.push(stats),
    });
    const cb = librdkafka["stats_cb"] as (raw: unknown) => void;
    cb({ already: "parsed" });
    expect(seen[0]).toEqual({ already: "parsed" });
  });

  it("stats_cb swallows JSON parse errors (loses one sample, does not crash producer loop)", () => {
    const seen: unknown[] = [];
    const { librdkafka } = buildConfluentClientConfig({
      brokers: ["b:9092"],
      onStats: (stats) => seen.push(stats),
    });
    const cb = librdkafka["stats_cb"] as (raw: string) => void;
    expect(() => cb("{not json")).not.toThrow();
    expect(seen).toHaveLength(0);
  });

  it("stats_cb swallows exceptions thrown by the user callback", () => {
    const { librdkafka } = buildConfluentClientConfig({
      brokers: ["b:9092"],
      onStats: () => {
        throw new Error("observer is buggy");
      },
    });
    const cb = librdkafka["stats_cb"] as (raw: string) => void;
    expect(() => cb('{"ok":true}')).not.toThrow();
  });
});
