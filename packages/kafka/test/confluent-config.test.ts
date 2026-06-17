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
