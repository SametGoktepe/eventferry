import { describe, expect, it } from "vitest";
import { compareLsn, lsnFromHex, lsnToHex, ZERO_LSN } from "../src/lsn.js";

// SQL Server CDC LSNs are 10-byte binary values (binary(10)) that order
// lexicographically. The helpers under test are the only sanctioned way to
// move between the on-the-wire Buffer form and the `0x...` hex string the
// `sys.fn_cdc_*` TVFs accept — so any bug here would silently corrupt
// resume positions in the watermark table.

const lsnA = Buffer.from([
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
]);
const lsnB = Buffer.from([
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
]);
const lsnHighByte = Buffer.from([
  0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
]);

describe("compareLsn", () => {
  it("returns 0 for equal buffers", () => {
    const a = Buffer.from(lsnA);
    const b = Buffer.from(lsnA);
    expect(compareLsn(a, b)).toBe(0);
  });

  it("returns 0 when the same buffer instance is compared to itself", () => {
    expect(compareLsn(lsnA, lsnA)).toBe(0);
  });

  it("returns -1 when left is lexicographically less than right", () => {
    expect(compareLsn(lsnA, lsnB)).toBe(-1);
  });

  it("returns 1 when left is lexicographically greater than right", () => {
    expect(compareLsn(lsnB, lsnA)).toBe(1);
  });

  it("orders by most-significant byte first (big-endian lex order)", () => {
    // lsnHighByte has 0x01 in byte 0; lsnB has 0x02 in byte 9.
    // Lex order puts lsnHighByte AFTER lsnB.
    expect(compareLsn(lsnHighByte, lsnB)).toBe(1);
    expect(compareLsn(lsnB, lsnHighByte)).toBe(-1);
  });

  it("treats ZERO_LSN as the minimum", () => {
    expect(compareLsn(ZERO_LSN, lsnA)).toBe(-1);
    expect(compareLsn(lsnA, ZERO_LSN)).toBe(1);
    expect(compareLsn(ZERO_LSN, ZERO_LSN)).toBe(0);
  });
});

describe("lsnToHex", () => {
  it("returns a 0x-prefixed uppercase 22-char string for a 10-byte buffer", () => {
    const hex = lsnToHex(lsnA);
    // "0x" + 20 hex digits == 22 chars total.
    expect(hex).toHaveLength(22);
    expect(hex.startsWith("0x")).toBe(true);
    // Everything after the prefix must be uppercase hex.
    expect(hex.slice(2)).toMatch(/^[0-9A-F]{20}$/);
  });

  it("encodes byte values correctly", () => {
    expect(lsnToHex(ZERO_LSN)).toBe("0x00000000000000000000");
    expect(lsnToHex(lsnA)).toBe("0x00000000000000000001");
    expect(lsnToHex(lsnHighByte)).toBe("0x01000000000000000000");
  });

  it("uppercases A-F digits", () => {
    const mixed = Buffer.from([
      0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde,
    ]);
    expect(lsnToHex(mixed)).toBe("0xABCDEF123456789ABCDE");
  });

  it("throws TypeError for null", () => {
    expect(() => lsnToHex(null as unknown as Buffer)).toThrow();
  });

  it("throws TypeError for undefined", () => {
    expect(() => lsnToHex(undefined as unknown as Buffer)).toThrow();
  });

  it("throws TypeError for a buffer of the wrong length", () => {
    expect(() => lsnToHex(Buffer.alloc(9))).toThrow();
    expect(() => lsnToHex(Buffer.alloc(11))).toThrow();
    expect(() => lsnToHex(Buffer.alloc(0))).toThrow();
  });

  it("throws TypeError for a non-Buffer input", () => {
    expect(() => lsnToHex("0x00000000000000000001" as unknown as Buffer)).toThrow(
      TypeError,
    );
  });
});

describe("lsnFromHex", () => {
  it("roundtrips lsnToHex(b) back to the original buffer bytes", () => {
    const samples: Buffer[] = [
      ZERO_LSN,
      lsnA,
      lsnB,
      lsnHighByte,
      Buffer.from([
        0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde,
      ]),
      Buffer.from([
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
      ]),
    ];
    for (const b of samples) {
      const roundtripped = lsnFromHex(lsnToHex(b));
      expect(Buffer.compare(roundtripped, b)).toBe(0);
    }
  });

  it("accepts uppercase hex with 0x prefix", () => {
    const out = lsnFromHex("0xABCDEF123456789ABCDE");
    expect(
      Buffer.compare(
        out,
        Buffer.from([
          0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde,
        ]),
      ),
    ).toBe(0);
  });

  it("throws TypeError for null", () => {
    expect(() => lsnFromHex(null as unknown as string)).toThrow();
  });

  it("throws TypeError for undefined", () => {
    expect(() => lsnFromHex(undefined as unknown as string)).toThrow();
  });

  it("throws TypeError for the wrong hex length", () => {
    // Missing one nibble.
    expect(() => lsnFromHex("0x0000000000000000000")).toThrow();
    // Extra nibble.
    expect(() => lsnFromHex("0x000000000000000000001")).toThrow();
    // Empty.
    expect(() => lsnFromHex("")).toThrow();
    // Bare prefix.
    expect(() => lsnFromHex("0x")).toThrow();
  });

  it("throws TypeError for non-hex characters", () => {
    // "Z" is not a hex digit.
    expect(() => lsnFromHex("0xZZZZZZZZZZZZZZZZZZZZ")).toThrow();
    // Embedded space.
    expect(() => lsnFromHex("0x0000000000000000000 ")).toThrow();
    // Right length but a single bad char.
    expect(() => lsnFromHex("0x000000000000000000G1")).toThrow();
  });

  it("throws TypeError for a non-string input", () => {
    expect(() => lsnFromHex(123 as unknown as string)).toThrow();
    expect(() => lsnFromHex(Buffer.alloc(10) as unknown as string)).toThrow(
      TypeError,
    );
  });
});

describe("ZERO_LSN", () => {
  it("is a Buffer of length 10", () => {
    expect(Buffer.isBuffer(ZERO_LSN)).toBe(true);
    expect(ZERO_LSN.length).toBe(10);
  });

  it("is all-zero bytes (equivalent to Buffer.alloc(10))", () => {
    expect(Buffer.compare(ZERO_LSN, Buffer.alloc(10))).toBe(0);
  });
});
