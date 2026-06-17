import type {
  OauthBearerToken,
  SaslOauthbearerConfig,
} from "@eventferry/kafka";

/**
 * AWS MSK IAM authentication for @eventferry/kafka.
 *
 * AWS MSK exposes IAM auth via SASL/OAUTHBEARER — the bearer token is an AWS
 * Signature V4 signature, not a JWT. This helper produces a token provider
 * compatible with eventferry's {@link SaslOauthbearerConfig.oauthBearerProvider}
 * shape, backed by the official `aws-msk-iam-sasl-signer-js` library.
 *
 * Wire it into the publisher's `sasl` option directly:
 *
 *   import { createMskIamSasl } from "@eventferry/kafka-iam";
 *
 *   new KafkaPublisher({
 *     brokers: ["b.cluster.us-east-1.amazonaws.com:9098"],
 *     ssl: true,
 *     sasl: createMskIamSasl({ region: "us-east-1" }),
 *   });
 */

/**
 * Minimal structural shape of the official AWS signer. The real module's
 * `generateAuthToken` matches this — we keep the interface here so the
 * helper compiles without `aws-msk-iam-sasl-signer-js` installed (it's
 * an optional peer).
 */
export interface MskIamSigner {
  generateAuthToken(opts: {
    region: string;
    // The signer accepts other fields (awsProfile, awsRoleArn, awsRoleSessionName);
    // we forward whatever the caller supplied via `signerOptions`.
    [k: string]: unknown;
  }): Promise<{ token: string; expiryTime: number }>;
}

/** Options forwarded to `aws-msk-iam-sasl-signer-js`'s `generateAuthToken`. */
export interface MskIamSaslOptions {
  /** AWS region of the MSK cluster (e.g. "us-east-1"). REQUIRED. */
  region: string;
  /**
   * Optional named AWS profile to use (resolved by the signer the same way
   * the AWS SDK does). Mutually exclusive with `awsRoleArn`.
   */
  awsProfile?: string;
  /**
   * Optional IAM role to assume before signing. The signer calls
   * `AssumeRole` itself. Mutually exclusive with `awsProfile`.
   */
  awsRoleArn?: string;
  /** Session name to use with `awsRoleArn`. Default `"MSKSASLDefaultSession"`. */
  awsRoleSessionName?: string;
  /**
   * Optional signer endpoint override. Useful for VPC endpoints / FIPS
   * regions. Forwarded as-is to the signer.
   */
  awsStsRegion?: string;
  /**
   * Optional pre-built signer (DI for tests). When set, the helper does NOT
   * dynamically import `aws-msk-iam-sasl-signer-js`.
   */
  signer?: MskIamSigner;
  /**
   * Optional override for the SASL principal. AWS MSK accepts any string;
   * default `"eventferry"` is the conventional name.
   */
  principal?: string;
  /**
   * Refresh-ahead window. Token is regenerated when its remaining lifetime
   * drops below this many milliseconds. Default 60_000 (60 s) — the
   * signer's default token lifetime is 15 minutes, so this leaves ~14 min
   * of usable cache between refreshes.
   */
  refreshAheadMs?: number;
}

/**
 * Construct the `sasl` block for `KafkaPublisher`. The returned config
 * memoizes the most recent token and refreshes it shortly before expiry —
 * librdkafka and kafkajs both invoke the provider on demand (no fixed
 * timer), so a process-local cache is the natural place for this.
 *
 * Concurrent invocations during a refresh dedupe onto a single in-flight
 * `generateAuthToken` call.
 */
export function createMskIamSasl(
  opts: MskIamSaslOptions,
): SaslOauthbearerConfig {
  if (!opts.region) {
    throw new Error("createMskIamSasl: `region` is required");
  }
  const refreshAheadMs = opts.refreshAheadMs ?? 60_000;
  const principal = opts.principal ?? "eventferry";

  let cached: { token: string; expiryMs: number } | null = null;
  let inFlight: Promise<OauthBearerToken> | null = null;
  let signer: MskIamSigner | null = opts.signer ?? null;

  async function refresh(): Promise<OauthBearerToken> {
    if (!signer) signer = await importDefaultSigner();
    const { region, awsProfile, awsRoleArn, awsRoleSessionName, awsStsRegion } =
      opts;
    const result = await signer.generateAuthToken({
      region,
      ...(awsProfile !== undefined ? { awsProfile } : {}),
      ...(awsRoleArn !== undefined ? { awsRoleArn } : {}),
      ...(awsRoleSessionName !== undefined ? { awsRoleSessionName } : {}),
      ...(awsStsRegion !== undefined ? { awsStsRegion } : {}),
    });
    // Some signers return expiry in seconds, some in ms. The official
    // aws-msk-iam-sasl-signer-js returns ms. Be defensive: anything below
    // year-2001 (in ms) is treated as seconds and rescaled.
    const expiryMs =
      result.expiryTime > 1_000_000_000_000
        ? result.expiryTime
        : result.expiryTime * 1000;
    cached = { token: result.token, expiryMs };
    return tokenFor(cached, principal);
  }

  return {
    mechanism: "oauthbearer",
    async oauthBearerProvider(): Promise<OauthBearerToken> {
      const now = Date.now();
      // Fast path: cached token still has plenty of headroom.
      if (cached && cached.expiryMs - now > refreshAheadMs) {
        return tokenFor(cached, principal);
      }
      // Dedup concurrent refresh attempts so a thundering herd hits the
      // signer once, not N times.
      if (!inFlight) {
        inFlight = refresh().finally(() => {
          inFlight = null;
        });
      }
      return inFlight;
    },
  };
}

function tokenFor(
  cached: { token: string; expiryMs: number },
  principal: string,
): OauthBearerToken {
  // Confluent driver requires {value, principal, lifetime}. kafkajs ignores
  // everything but `value` — supplying the extras is a no-op there.
  // Lifetime is in MILLISECONDS per the eventferry contract.
  const lifetime = Math.max(cached.expiryMs - Date.now(), 0);
  return {
    value: cached.token,
    principal,
    lifetime,
  };
}

async function importDefaultSigner(): Promise<MskIamSigner> {
  try {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const mod: any = await import("aws-msk-iam-sasl-signer-js");
    // The package exports either `generateAuthToken` directly OR a default
    // object containing it. Normalize both shapes into an MskIamSigner.
    if (typeof mod?.generateAuthToken === "function") {
      return { generateAuthToken: mod.generateAuthToken };
    }
    if (typeof mod?.default?.generateAuthToken === "function") {
      return { generateAuthToken: mod.default.generateAuthToken };
    }
    throw new Error("module shape mismatch — no generateAuthToken export found");
  } catch (err) {
    throw new Error(
      'createMskIamSasl requires the "aws-msk-iam-sasl-signer-js" package. ' +
        'Run: npm i aws-msk-iam-sasl-signer-js. ' +
        `(underlying error: ${(err as Error).message})`,
    );
  }
}
