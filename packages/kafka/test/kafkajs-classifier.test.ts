import { describe, expect, it } from "vitest";
import { classifyKafkajsError } from "../src/kafkajs-classifier.js";

describe("classifyKafkajsError — class-based shortcuts", () => {
  it("KafkaJSConnectionError → retriable", () => {
    const e = Object.assign(new Error("EHOSTUNREACH"), {
      name: "KafkaJSConnectionError",
    });
    expect(classifyKafkajsError(e)).toBe("retriable");
  });

  it("KafkaJSRequestTimeoutError → retriable", () => {
    const e = Object.assign(new Error("timed out"), {
      name: "KafkaJSRequestTimeoutError",
    });
    expect(classifyKafkajsError(e)).toBe("retriable");
  });

  it("KafkaJSNonRetriableError → fatal", () => {
    const e = Object.assign(new Error("nope"), {
      name: "KafkaJSNonRetriableError",
    });
    expect(classifyKafkajsError(e)).toBe("fatal");
  });
});

describe("classifyKafkajsError — protocol type strings", () => {
  const cases: Array<{ type: string; want: string }> = [
    { type: "NOT_LEADER_FOR_PARTITION", want: "retriable" },
    { type: "LEADER_NOT_AVAILABLE", want: "retriable" },
    { type: "UNKNOWN_TOPIC_OR_PARTITION", want: "retriable" },
    { type: "REQUEST_TIMED_OUT", want: "retriable" },
    { type: "REPLICA_NOT_AVAILABLE", want: "retriable" },
    { type: "NOT_ENOUGH_REPLICAS", want: "retriable" },
    { type: "FENCED_LEADER_EPOCH", want: "retriable" },
    { type: "CORRUPT_MESSAGE", want: "poison" },
    { type: "MESSAGE_TOO_LARGE", want: "poison" },
    { type: "INVALID_RECORD", want: "poison" },
    { type: "UNSUPPORTED_COMPRESSION_TYPE", want: "poison" },
    { type: "INVALID_PRODUCER_EPOCH", want: "fatal" },
    { type: "PRODUCER_FENCED", want: "fatal" },
    { type: "TOPIC_AUTHORIZATION_FAILED", want: "fatal" },
    { type: "CLUSTER_AUTHORIZATION_FAILED", want: "fatal" },
    { type: "SASL_AUTHENTICATION_FAILED", want: "fatal" },
  ];
  it.each(cases)("$type → $want", ({ type, want }) => {
    expect(classifyKafkajsError({ name: "KafkaJSProtocolError", type })).toBe(
      want,
    );
  });
});

describe("classifyKafkajsError — numeric code fallback", () => {
  const cases: Array<{ code: number; want: string }> = [
    { code: 3, want: "retriable" }, // UNKNOWN_TOPIC_OR_PARTITION
    { code: 5, want: "retriable" }, // LEADER_NOT_AVAILABLE
    { code: 6, want: "retriable" }, // NOT_LEADER_FOR_PARTITION
    { code: 7, want: "retriable" }, // REQUEST_TIMED_OUT
    { code: 10, want: "poison" }, // MESSAGE_TOO_LARGE
    { code: 29, want: "fatal" }, // TOPIC_AUTHORIZATION_FAILED
    { code: 47, want: "fatal" }, // INVALID_PRODUCER_EPOCH
    { code: 58, want: "fatal" }, // SASL_AUTHENTICATION_FAILED
    { code: 87, want: "poison" }, // INVALID_RECORD
  ];
  it.each(cases)("code $code → $want", ({ code, want }) => {
    expect(classifyKafkajsError({ name: "KafkaJSProtocolError", code })).toBe(
      want,
    );
  });
});

describe("classifyKafkajsError — defaults & junk", () => {
  it("unknown protocol type falls back to retriable", () => {
    expect(
      classifyKafkajsError({ name: "KafkaJSProtocolError", type: "MADE_UP" }),
    ).toBe("retriable");
  });

  it("unknown code falls back to retriable", () => {
    expect(classifyKafkajsError({ name: "KafkaJSProtocolError", code: 9999 })).toBe(
      "retriable",
    );
  });

  it("null / undefined → retriable", () => {
    expect(classifyKafkajsError(null)).toBe("retriable");
    expect(classifyKafkajsError(undefined)).toBe("retriable");
  });

  it("plain string → retriable", () => {
    expect(classifyKafkajsError("oops")).toBe("retriable");
  });

  it("plain Error with no fields → retriable", () => {
    expect(classifyKafkajsError(new Error("oops"))).toBe("retriable");
  });
});
