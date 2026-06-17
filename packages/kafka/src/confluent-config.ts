import type {
  KafkaConnectionConfig,
  LibrdkafkaStats,
  ProducerBehaviorConfig,
  TlsConfig,
} from "./driver.js";

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
  opts: KafkaConnectionConfig & ProducerBehaviorConfig,
): ConfluentClientConfig {
  const kafkaJS: Record<string, unknown> = {
    clientId: opts.clientId ?? "eventferry",
    brokers: opts.brokers,
  };
  const librdkafka: Record<string, unknown> = {};

  // ── Producer tuning passthrough (librdkafka config keys) ─────────────
  if (opts.lingerMs !== undefined) librdkafka["linger.ms"] = opts.lingerMs;
  if (opts.batchSize !== undefined) librdkafka["batch.size"] = opts.batchSize;
  if (opts.maxInFlightRequests !== undefined) {
    librdkafka["max.in.flight.requests.per.connection"] =
      opts.maxInFlightRequests;
  }
  if (opts.requestTimeoutMs !== undefined) {
    librdkafka["request.timeout.ms"] = opts.requestTimeoutMs;
  }
  if (opts.deliveryTimeoutMs !== undefined) {
    librdkafka["delivery.timeout.ms"] = opts.deliveryTimeoutMs;
  }
  if (opts.maxRequestSize !== undefined) {
    librdkafka["message.max.bytes"] = opts.maxRequestSize;
  }
  if (opts.transactionTimeoutMs !== undefined) {
    librdkafka["transaction.timeout.ms"] = opts.transactionTimeoutMs;
  }
  if (opts.compressionLevel !== undefined) {
    librdkafka["compression.level"] = opts.compressionLevel;
  }

  // Stats hook: wire `stats_cb` into librdkafka and pick a sensible default
  // interval. librdkafka stats are CPU-billed (it serializes the stats JSON
  // each tick) so we don't enable the timer unless the hook is set.
  if (opts.onStats) {
    librdkafka["stats_cb"] = wrapStatsCallback(opts.onStats);
    // Default 30s — long enough to be cheap, short enough to be useful.
    // Users can override via statsIntervalMs OR rawProducerConfig.
    librdkafka["statistics.interval.ms"] =
      opts.statsIntervalMs ?? 30_000;
  } else if (opts.statsIntervalMs !== undefined) {
    // statsIntervalMs without onStats is a no-op at the eventferry layer
    // (librdkafka still emits stats events, just discarded). Honor the
    // setting in case the user wired their own event listener on the
    // raw client via rawProducerConfig.
    librdkafka["statistics.interval.ms"] = opts.statsIntervalMs;
  }

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

  // Power-user escape hatch — merged LAST so raw keys win against the
  // translated ones (deliberate; that's the whole point of the hatch).
  if (opts.rawProducerConfig) {
    Object.assign(librdkafka, opts.rawProducerConfig);
  }

  return { kafkaJS, librdkafka };
}

function isTlsConfig(v: unknown): v is TlsConfig {
  return typeof v === "object" && v !== null;
}

/**
 * Wrap the user's onStats callback to:
 *   1. Parse the JSON string librdkafka emits (it's never an object).
 *   2. Swallow callback exceptions — a misbehaving observer must never
 *      take down the producer's event loop.
 *   3. Swallow JSON parse failures with no further escalation — losing a
 *      single stats sample is preferable to an unhandled exception in a
 *      hot-path emitter that fires every {@link ProducerBehaviorConfig.statsIntervalMs}.
 */
function wrapStatsCallback(
  onStats: (stats: LibrdkafkaStats) => void,
): (raw: string | LibrdkafkaStats) => void {
  return (raw) => {
    let parsed: LibrdkafkaStats;
    try {
      parsed = typeof raw === "string" ? (JSON.parse(raw) as LibrdkafkaStats) : raw;
    } catch {
      return;
    }
    try {
      onStats(parsed);
    } catch {
      // user-supplied callback threw; ignore — the producer loop must
      // continue. Use a logger inside your callback if you want diagnostics.
    }
  };
}

function stringifyPem(input: string | Buffer | Array<string | Buffer>): string {
  if (Array.isArray(input)) {
    return input.map((x) => (typeof x === "string" ? x : x.toString("utf8"))).join("\n");
  }
  return typeof input === "string" ? input : input.toString("utf8");
}
