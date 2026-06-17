import { describe, expect, it } from "vitest";
import {
  decode,
  decodeHeaders,
  extractTraceContext,
  type IncomingKafkaMessage,
} from "../src/consume.js";

describe("decodeHeaders", () => {
  it("returns an empty object when headers are missing", () => {
    expect(decodeHeaders()).toEqual({});
    expect(decodeHeaders(undefined)).toEqual({});
  });

  it("normalizes Buffer values to UTF-8 strings", () => {
    expect(
      decodeHeaders({
        "x-trace": Buffer.from("abc-123", "utf8"),
        "x-tenant": "tenant-7",
      }),
    ).toEqual({
      "x-trace": "abc-123",
      "x-tenant": "tenant-7",
    });
  });

  it("drops undefined entries (some consumers surface absent headers as undefined)", () => {
    expect(
      decodeHeaders({ keep: "yes", drop: undefined, alsoKeep: "yes" }),
    ).toEqual({ keep: "yes", alsoKeep: "yes" });
  });
});

describe("decode", () => {
  function msg(over: Partial<IncomingKafkaMessage> = {}): IncomingKafkaMessage {
    return {
      key: "agg-1",
      value: Buffer.from(JSON.stringify({ orderId: 7 })),
      headers: { "x-tenant": "t1" },
      offset: "42",
      timestamp: "1700000000000",
      partition: 3,
      ...over,
    };
  }

  it("default JSON decoder parses the payload", () => {
    const out = decode<{ orderId: number }>(msg());
    expect(out.value).toEqual({ orderId: 7 });
    expect(out.key).toBe("agg-1");
    expect(out.headers).toEqual({ "x-tenant": "t1" });
    expect(out.offset).toBe("42");
    expect(out.timestamp).toBe(1700000000000);
    expect(out.partition).toBe(3);
  });

  it('decoder: "utf8" returns the raw string', () => {
    const out = decode(msg({ value: Buffer.from("plain text") }), {
      decoder: "utf8",
    });
    expect(out.value).toBe("plain text");
  });

  it('decoder: "none" returns the raw Buffer', () => {
    const buf = Buffer.from([0x01, 0x02, 0x03]);
    const out = decode<Buffer>(msg({ value: buf }), { decoder: "none" });
    expect(Buffer.isBuffer(out.value)).toBe(true);
    expect(out.value).toEqual(buf);
  });

  it("custom decoder function receives the raw bytes", () => {
    const out = decode(msg({ value: Buffer.from("hello") }), {
      decoder: (b) => b.toString("hex"),
    });
    expect(out.value).toBe("68656c6c6f");
  });

  it("null value (Kafka tombstone) decodes to null regardless of decoder", () => {
    expect(decode(msg({ value: null })).value).toBeNull();
    expect(decode(msg({ value: null }), { decoder: "none" }).value).toBeNull();
    expect(decode(msg({ value: null }), { decoder: "utf8" }).value).toBeNull();
  });

  it("empty Buffer value also decodes to null (compaction tombstone variant)", () => {
    expect(decode(msg({ value: Buffer.alloc(0) })).value).toBeNull();
  });

  it("Buffer key is converted to a string", () => {
    const out = decode(msg({ key: Buffer.from("buffer-key") }));
    expect(out.key).toBe("buffer-key");
  });

  it("absent key becomes null (kafkajs delivers null for keyless records)", () => {
    expect(decode(msg({ key: null })).key).toBeNull();
    expect(decode(msg({ key: undefined })).key).toBeNull();
  });

  it("JSON decoder throws a labelled error on malformed payloads", () => {
    expect(() =>
      decode(msg({ value: Buffer.from("{not json") })),
    ).toThrow(/JSON\.parse failed/);
  });

  it("numeric offset and timestamp are normalized", () => {
    const out = decode(msg({ offset: 99, timestamp: 1700000000000 }));
    expect(out.offset).toBe("99");
    expect(out.timestamp).toBe(1700000000000);
  });
});

describe("extractTraceContext", () => {
  const validTp =
    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

  it("returns null when headers are missing", () => {
    expect(extractTraceContext(undefined)).toBeNull();
    expect(extractTraceContext({})).toBeNull();
  });

  it("returns null when traceparent header is absent", () => {
    expect(extractTraceContext({ "x-other": "value" })).toBeNull();
  });

  it("parses a valid sampled traceparent", () => {
    const ctx = extractTraceContext({ traceparent: validTp });
    expect(ctx).toEqual({
      traceparent: validTp,
      traceId: "4bf92f3577b34da6a3ce929d0e0e4736",
      spanId: "00f067aa0ba902b7",
      sampled: true,
      tracestate: undefined,
    });
  });

  it("parses unsampled flag (00) correctly", () => {
    const tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00";
    expect(extractTraceContext({ traceparent: tp })?.sampled).toBe(false);
  });

  it("surfaces tracestate when present", () => {
    const ctx = extractTraceContext({
      traceparent: validTp,
      tracestate: "vendor=value,other=v2",
    });
    expect(ctx?.tracestate).toBe("vendor=value,other=v2");
  });

  it("reads traceparent from Buffer headers (pre-decode call)", () => {
    const ctx = extractTraceContext({ traceparent: Buffer.from(validTp) });
    expect(ctx?.traceId).toBe("4bf92f3577b34da6a3ce929d0e0e4736");
  });

  it('rejects forbidden version "ff"', () => {
    const tp = "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    expect(extractTraceContext({ traceparent: tp })).toBeNull();
  });

  it("rejects all-zero trace id", () => {
    const tp = "00-00000000000000000000000000000000-00f067aa0ba902b7-01";
    expect(extractTraceContext({ traceparent: tp })).toBeNull();
  });

  it("rejects all-zero span id", () => {
    const tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01";
    expect(extractTraceContext({ traceparent: tp })).toBeNull();
  });

  it("rejects malformed traceparent (wrong segment lengths)", () => {
    expect(
      extractTraceContext({ traceparent: "00-shortid-00f067aa0ba902b7-01" }),
    ).toBeNull();
    expect(extractTraceContext({ traceparent: "not-a-traceparent" })).toBeNull();
  });

  it("rejects non-hex characters", () => {
    const tp = "00-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz-00f067aa0ba902b7-01";
    expect(extractTraceContext({ traceparent: tp })).toBeNull();
  });
});
