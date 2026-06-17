import { describe, expect, it, vi } from "vitest";
import {
  createMskIamSasl,
  type MskIamSigner,
} from "../src/iam-provider.js";

function fakeSigner(
  overrides: Partial<{
    token: string;
    expiryTime: number;
    calls: string[];
    delay: number;
  }> = {},
): MskIamSigner & { calls: string[] } {
  const calls: string[] = [];
  return {
    calls,
    async generateAuthToken(opts) {
      calls.push(JSON.stringify(opts));
      if (overrides.delay) {
        await new Promise((r) => setTimeout(r, overrides.delay));
      }
      return {
        token: overrides.token ?? "fake-token-" + calls.length,
        expiryTime: overrides.expiryTime ?? Date.now() + 15 * 60 * 1000,
      };
    },
  };
}

describe("createMskIamSasl", () => {
  it("throws when region is missing", () => {
    expect(() =>
      // @ts-expect-error: testing runtime guard
      createMskIamSasl({}),
    ).toThrow(/region/);
  });

  it("returns a SASL/OAUTHBEARER config with the eventferry shape", async () => {
    const signer = fakeSigner();
    const cfg = createMskIamSasl({ region: "us-east-1", signer });
    expect(cfg.mechanism).toBe("oauthbearer");
    expect(typeof cfg.oauthBearerProvider).toBe("function");
    const token = await cfg.oauthBearerProvider();
    expect(token.value).toBe("fake-token-1");
    // Confluent driver requires principal + lifetime; default principal
    // is "eventferry" so adoption is zero-config out of the box.
    expect(token.principal).toBe("eventferry");
    expect(token.lifetime).toBeGreaterThan(0);
  });

  it("forwards region + IAM profile/role options to the signer", async () => {
    const signer = fakeSigner();
    const cfg = createMskIamSasl({
      region: "eu-west-2",
      awsProfile: "prod",
      signer,
    });
    await cfg.oauthBearerProvider();
    expect(signer.calls).toHaveLength(1);
    const parsed = JSON.parse(signer.calls[0]!);
    expect(parsed.region).toBe("eu-west-2");
    expect(parsed.awsProfile).toBe("prod");
    // We omit undefined-valued keys so the signer sees a clean object.
    expect(Object.keys(parsed).sort()).toEqual(["awsProfile", "region"]);
  });

  it("caches the token until the refresh-ahead window kicks in", async () => {
    const signer = fakeSigner({
      expiryTime: Date.now() + 15 * 60 * 1000, // 15 min lifetime
    });
    const cfg = createMskIamSasl({
      region: "us-east-1",
      signer,
      refreshAheadMs: 60_000,
    });
    const a = await cfg.oauthBearerProvider();
    const b = await cfg.oauthBearerProvider();
    const c = await cfg.oauthBearerProvider();
    expect(signer.calls).toHaveLength(1);
    expect(a.value).toBe(b.value);
    expect(b.value).toBe(c.value);
  });

  it("refreshes when the cached token's remaining lifetime is below refreshAheadMs", async () => {
    // Start with a token that expires in 30 s. refreshAhead is 60 s — every
    // call should trigger a refresh because the cached token is already in
    // the refresh window.
    const signer = fakeSigner({ expiryTime: Date.now() + 30_000 });
    const cfg = createMskIamSasl({
      region: "us-east-1",
      signer,
      refreshAheadMs: 60_000,
    });
    await cfg.oauthBearerProvider();
    await cfg.oauthBearerProvider();
    await cfg.oauthBearerProvider();
    expect(signer.calls).toHaveLength(3);
  });

  it("dedupes concurrent refresh attempts onto a single signer call", async () => {
    const signer = fakeSigner({ delay: 30 });
    const cfg = createMskIamSasl({ region: "us-east-1", signer });
    const [a, b, c] = await Promise.all([
      cfg.oauthBearerProvider(),
      cfg.oauthBearerProvider(),
      cfg.oauthBearerProvider(),
    ]);
    expect(signer.calls).toHaveLength(1);
    expect(a.value).toBe(b.value);
    expect(b.value).toBe(c.value);
  });

  it("rescales expiryTime when the signer returns seconds (defensive)", async () => {
    const nowSec = Math.floor(Date.now() / 1000) + 900;
    const signer = fakeSigner({ expiryTime: nowSec });
    const cfg = createMskIamSasl({
      region: "us-east-1",
      signer,
      // Force a long refresh-ahead so a too-soon-expiring token would
      // refetch on next call — verifies the rescale actually worked.
      refreshAheadMs: 60_000,
    });
    await cfg.oauthBearerProvider();
    // 15 min of headroom after rescale → second call should be cached.
    await cfg.oauthBearerProvider();
    expect(signer.calls).toHaveLength(1);
  });

  it("allows overriding the SASL principal", async () => {
    const signer = fakeSigner();
    const cfg = createMskIamSasl({
      region: "us-east-1",
      principal: "my-app",
      signer,
    });
    const token = await cfg.oauthBearerProvider();
    expect(token.principal).toBe("my-app");
  });

  it("surfaces signer errors to the caller (does not swallow)", async () => {
    const signer: MskIamSigner = {
      async generateAuthToken() {
        throw new Error("IAM credentials missing");
      },
    };
    const cfg = createMskIamSasl({ region: "us-east-1", signer });
    await expect(cfg.oauthBearerProvider()).rejects.toThrow(
      /IAM credentials missing/,
    );
  });

  it("recovers after a transient signer failure (in-flight slot is cleared)", async () => {
    let attempt = 0;
    const signer: MskIamSigner = {
      async generateAuthToken() {
        attempt++;
        if (attempt === 1) throw new Error("transient");
        return { token: "ok", expiryTime: Date.now() + 15 * 60 * 1000 };
      },
    };
    const cfg = createMskIamSasl({ region: "us-east-1", signer });
    await expect(cfg.oauthBearerProvider()).rejects.toThrow(/transient/);
    const token = await cfg.oauthBearerProvider();
    expect(token.value).toBe("ok");
    expect(attempt).toBe(2);
  });
});
