import { describe, expect, it, vi } from "vitest";
import {
  SchemaRegistrySerializer,
  bearerAuthMiddleware,
} from "../src/serializer.js";

// Mock the optional peer so we can capture the constructor config the
// serializer would hand to it. Lives at the file level — vitest hoists
// vi.mock() calls above the imports so this rewires module resolution
// before SchemaRegistrySerializer reaches in dynamically.
const capturedConstructorCfg: Array<Record<string, unknown>> = [];

vi.mock("@kafkajs/confluent-schema-registry", () => ({
  SchemaRegistry: class {
    constructor(cfg: Record<string, unknown>) {
      capturedConstructorCfg.push(cfg);
    }
    // Stub the surface the serializer exercises — never called in these tests,
    // they only assert on the constructor cfg.
    async register() {
      return { id: 1 };
    }
    async getLatestSchemaId() {
      return 1;
    }
    async encode() {
      return Buffer.alloc(0);
    }
  },
}));

describe("SchemaRegistrySerializer — auth wiring (host path)", () => {
  function freshSerializer(
    opts: Parameters<typeof SchemaRegistrySerializer>[0],
  ) {
    capturedConstructorCfg.length = 0;
    return new SchemaRegistrySerializer(opts);
  }

  it("basic auth: forwards { username, password } to the SR constructor", async () => {
    const serializer = freshSerializer({
      host: "https://sr.example.com",
      auth: { type: "basic", username: "alice", password: "s3cret" },
    });
    await serializer.serialize({
      id: "1",
      messageId: "m1",
      topic: "t",
      aggregateType: "a",
      aggregateId: "a1",
      key: null,
      payload: { v: 1 },
      headers: {},
      traceId: null,
      status: "pending",
      attempts: 0,
      nextRetryAt: null,
      createdAt: new Date(),
      processedAt: null,
    });
    expect(capturedConstructorCfg).toHaveLength(1);
    expect(capturedConstructorCfg[0]?.host).toBe("https://sr.example.com");
    expect(capturedConstructorCfg[0]?.auth).toEqual({
      username: "alice",
      password: "s3cret",
    });
    expect(capturedConstructorCfg[0]?.middlewares).toBeUndefined();
  });

  it("bearer auth: passes a middleware (NOT an `auth` field)", async () => {
    const serializer = freshSerializer({
      host: "https://sr.example.com",
      auth: { type: "bearer", token: "tok-123" },
    });
    await serializer.serialize({
      id: "1",
      messageId: "m1",
      topic: "t",
      aggregateType: "a",
      aggregateId: "a1",
      key: null,
      payload: { v: 1 },
      headers: {},
      traceId: null,
      status: "pending",
      attempts: 0,
      nextRetryAt: null,
      createdAt: new Date(),
      processedAt: null,
    });
    expect(capturedConstructorCfg[0]?.auth).toBeUndefined();
    const middlewares = capturedConstructorCfg[0]?.middlewares as unknown[];
    expect(Array.isArray(middlewares)).toBe(true);
    expect(middlewares).toHaveLength(1);
    expect(typeof middlewares[0]).toBe("function");
  });

  it("no auth: SR constructor gets only `host` (no auth/middleware noise)", async () => {
    const serializer = freshSerializer({ host: "https://sr.example.com" });
    await serializer.serialize({
      id: "1",
      messageId: "m1",
      topic: "t",
      aggregateType: "a",
      aggregateId: "a1",
      key: null,
      payload: { v: 1 },
      headers: {},
      traceId: null,
      status: "pending",
      attempts: 0,
      nextRetryAt: null,
      createdAt: new Date(),
      processedAt: null,
    });
    expect(capturedConstructorCfg[0]?.auth).toBeUndefined();
    expect(capturedConstructorCfg[0]?.middlewares).toBeUndefined();
  });
});

describe("bearerAuthMiddleware", () => {
  // Minimal stub matching the mappersmith Request#enhance shape.
  function fakeRequest(): {
    headers: Record<string, string>;
    enhance: (args: { headers?: Record<string, string> }) => unknown;
  } {
    const req = {
      headers: {} as Record<string, string>,
      enhance(args: { headers?: Record<string, string> }) {
        Object.assign(req.headers, args.headers ?? {});
        return req;
      },
    };
    return req;
  }

  it("attaches Authorization: Bearer <token> from a static string", async () => {
    const factory = bearerAuthMiddleware("static-token");
    const mw = factory();
    const req = fakeRequest();
    await mw.prepareRequest(async () => req);
    expect(req.headers.Authorization).toBe("Bearer static-token");
  });

  it("calls the callable token provider on EVERY request", async () => {
    const provider = vi.fn(() => "rotating-token");
    const factory = bearerAuthMiddleware(provider);
    const mw = factory();
    await mw.prepareRequest(async () => fakeRequest());
    await mw.prepareRequest(async () => fakeRequest());
    await mw.prepareRequest(async () => fakeRequest());
    expect(provider).toHaveBeenCalledTimes(3);
  });

  it("awaits an async token provider", async () => {
    const factory = bearerAuthMiddleware(async () => "async-token");
    const mw = factory();
    const req = fakeRequest();
    await mw.prepareRequest(async () => req);
    expect(req.headers.Authorization).toBe("Bearer async-token");
  });

  it("constructor opts.auth is IGNORED when an injected `registry` is provided", async () => {
    // Inline fake client; the serializer should NOT spin up the upstream
    // constructor when registry is injected, so auth has no effect.
    const fake = {
      async register() {
        return { id: 7 };
      },
      async getLatestSchemaId() {
        return 7;
      },
      async encode() {
        return Buffer.from("ok");
      },
    };
    capturedConstructorCfg.length = 0;
    const serializer = new SchemaRegistrySerializer({
      registry: fake,
      auth: { type: "basic", username: "ignored", password: "ignored" },
    });
    await serializer.serialize({
      id: "1",
      messageId: "m1",
      topic: "t",
      aggregateType: "a",
      aggregateId: "a1",
      key: null,
      payload: { v: 1 },
      headers: {},
      traceId: null,
      status: "pending",
      attempts: 0,
      nextRetryAt: null,
      createdAt: new Date(),
      processedAt: null,
    });
    expect(capturedConstructorCfg).toHaveLength(0); // mock NOT invoked
  });
});
