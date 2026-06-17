import type { KafkaConnectionConfig, TlsConfig } from "./driver.js";

/**
 * Translate eventferry's normalized `KafkaConnectionConfig` into the shape
 * expected by `@confluentinc/kafka-javascript`'s `KafkaJS.Kafka` constructor.
 *
 * Returns an object with two parts:
 *   - `kafkaJS`: the kafkajs-compatible config layer (clientId, brokers, and
 *     simple ssl/sasl when no advanced TLS is needed).
 *   - top-level keys: librdkafka-style config (e.g. `ssl.ca.pem`,
 *     `security.protocol`) used when the user supplies a {@link TlsConfig}.
 *
 * Why a separate translator: the kafkajs-compat layer accepts the simple
 * `ssl: true` boolean but the verified path for mTLS (CA + cert + key) is
 * librdkafka's `ssl.*.pem` keys. The translator picks the right surface
 * based on what the caller supplied. Buffer inputs are coerced to strings —
 * librdkafka accepts PEM strings, NOT Buffers.
 */
export interface ConfluentClientConfig {
  kafkaJS: Record<string, unknown>;
  // librdkafka top-level keys; kept as a Record so we can spread them.
  librdkafka: Record<string, unknown>;
}

export function buildConfluentClientConfig(
  opts: KafkaConnectionConfig,
): ConfluentClientConfig {
  const kafkaJS: Record<string, unknown> = {
    clientId: opts.clientId ?? "eventferry",
    brokers: opts.brokers,
  };
  const librdkafka: Record<string, unknown> = {};

  const tlsRequested = opts.ssl === true || isTlsConfig(opts.ssl);
  const saslRequested = !!opts.sasl;

  if (saslRequested && tlsRequested) {
    librdkafka["security.protocol"] = "sasl_ssl";
  } else if (tlsRequested) {
    librdkafka["security.protocol"] = "ssl";
  } else if (saslRequested) {
    librdkafka["security.protocol"] = "sasl_plaintext";
  } // else: leave as default (plaintext)

  if (isTlsConfig(opts.ssl)) {
    // Custom TLS — explicit librdkafka PEM keys. Buffers are coerced to
    // strings (librdkafka does not accept Buffer).
    const tls = opts.ssl;
    if (tls.ca !== undefined) {
      librdkafka["ssl.ca.pem"] = stringifyPem(tls.ca);
    }
    if (tls.cert !== undefined) {
      librdkafka["ssl.certificate.pem"] = stringifyPem(tls.cert);
    }
    if (tls.key !== undefined) {
      librdkafka["ssl.key.pem"] = stringifyPem(tls.key);
    }
    if (tls.passphrase !== undefined) {
      librdkafka["ssl.key.password"] = tls.passphrase;
    }
    // servername (SNI) — librdkafka derives SNI from `ssl.endpoint.identification.algorithm`;
    // explicit SNI override is not documented in the v1.x kafkaJS-compat surface, so we
    // honor it as a no-op for now and document the limitation in the gap analysis.
  } else if (opts.ssl === true) {
    // Simple TLS — kafkajs-compat boolean is sufficient.
    kafkaJS["ssl"] = true;
  }

  if (opts.sasl) {
    // SASL — kafkajs-compat shape works for both password mechanisms and
    // OAUTHBEARER (the confluent client implements the provider callback).
    kafkaJS["sasl"] = opts.sasl;
  }

  return { kafkaJS, librdkafka };
}

function isTlsConfig(v: unknown): v is TlsConfig {
  return typeof v === "object" && v !== null;
}

function stringifyPem(input: string | Buffer | Array<string | Buffer>): string {
  if (Array.isArray(input)) {
    return input.map((x) => (typeof x === "string" ? x : x.toString("utf8"))).join("\n");
  }
  return typeof input === "string" ? input : input.toString("utf8");
}
