import { describe, expect, it } from "vitest";
import { classifyConfluentError } from "../src/confluent-classifier.js";

describe("classifyConfluentError — librdkafka internal codes (negative)", () => {
  const cases: Array<{ code: number; want: string; label: string }> = [
    { code: -184, want: "backpressure", label: "ERR__QUEUE_FULL" },
    { code: -185, want: "retriable", label: "ERR__TIMED_OUT" },
    { code: -187, want: "retriable", label: "ERR__ALL_BROKERS_DOWN" },
    { code: -188, want: "poison", label: "ERR__UNKNOWN_TOPIC" },
    { code: -190, want: "poison", label: "ERR__UNKNOWN_PARTITION" },
    { code: -192, want: "retriable", label: "ERR__MSG_TIMED_OUT" },
    { code: -195, want: "retriable", label: "ERR__TRANSPORT" },
    { code: -198, want: "poison", label: "ERR__BAD_COMPRESSION" },
    { code: -144, want: "fatal", label: "ERR__FENCED" },
    { code: -150, want: "fatal", label: "ERR__FATAL" },
    { code: -169, want: "fatal", label: "ERR__AUTHENTICATION" },
    { code: -181, want: "fatal", label: "ERR__SSL" },
    { code: -196, want: "retriable", label: "ERR__FAIL (catch-all)" },
  ];
  it.each(cases)("$label ($code) → $want", ({ code, want }) => {
    expect(classifyConfluentError({ code })).toBe(want);
  });
});

describe("classifyConfluentError — wire protocol codes", () => {
  const cases: Array<{ code: number; want: string; label: string }> = [
    { code: 2, want: "poison", label: "CORRUPT_MESSAGE" },
    { code: 3, want: "retriable", label: "UNKNOWN_TOPIC_OR_PARTITION" },
    { code: 5, want: "retriable", label: "LEADER_NOT_AVAILABLE" },
    { code: 6, want: "retriable", label: "NOT_LEADER_FOR_PARTITION" },
    { code: 7, want: "retriable", label: "REQUEST_TIMED_OUT" },
    { code: 10, want: "poison", label: "MESSAGE_TOO_LARGE" },
    { code: 13, want: "retriable", label: "NETWORK_EXCEPTION" },
    { code: 29, want: "fatal", label: "TOPIC_AUTHORIZATION_FAILED" },
    { code: 31, want: "fatal", label: "CLUSTER_AUTHORIZATION_FAILED" },
    { code: 47, want: "fatal", label: "INVALID_PRODUCER_EPOCH" },
    { code: 58, want: "fatal", label: "SASL_AUTHENTICATION_FAILED" },
    { code: 76, want: "poison", label: "UNSUPPORTED_COMPRESSION_TYPE" },
    { code: 87, want: "poison", label: "INVALID_RECORD" },
    { code: 89, want: "quota", label: "THROTTLING_QUOTA_EXCEEDED" },
  ];
  it.each(cases)("$label ($code) → $want", ({ code, want }) => {
    expect(classifyConfluentError({ code })).toBe(want);
  });
});

describe("classifyConfluentError — symbolic name fallback", () => {
  const cases: Array<{ name: string; want: string }> = [
    { name: "ERR__QUEUE_FULL", want: "backpressure" },
    { name: "ERR__FENCED", want: "fatal" },
    { name: "ERR__FATAL", want: "fatal" },
    { name: "ERR_TOPIC_AUTHORIZATION_FAILED", want: "fatal" },
    { name: "ERR_MSG_SIZE_TOO_LARGE", want: "poison" },
    { name: "ERR_THROTTLING_QUOTA_EXCEEDED", want: "quota" },
  ];
  it.each(cases)("$name → $want", ({ name, want }) => {
    // No `code`, only name — exercises NAME_TO_KIND lookup.
    expect(classifyConfluentError({ name })).toBe(want);
  });
});

describe("classifyConfluentError — defaults & junk", () => {
  it("unknown numeric code → retriable", () => {
    expect(classifyConfluentError({ code: 9999 })).toBe("retriable");
  });
  it("unknown symbolic name → retriable", () => {
    expect(classifyConfluentError({ name: "ERR_MADE_UP" })).toBe("retriable");
  });
  it("null / undefined → retriable", () => {
    expect(classifyConfluentError(null)).toBe("retriable");
    expect(classifyConfluentError(undefined)).toBe("retriable");
  });
  it("plain string → retriable", () => {
    expect(classifyConfluentError("oops")).toBe("retriable");
  });
  it("plain Error with no fields → retriable", () => {
    expect(classifyConfluentError(new Error("oops"))).toBe("retriable");
  });
});
