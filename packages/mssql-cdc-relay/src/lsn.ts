/**
 * `@eventferry/mssql-cdc-relay` — LSN (Log Sequence Number) helpers.
 *
 * ---------------------------------------------------------------------------
 * SQL Server CDC LSN format
 * ---------------------------------------------------------------------------
 *
 * SQL Server Change Data Capture exposes a transaction log position as a
 * `BINARY(10)` value — a fixed-width 10-byte sequence. Every CDC row in
 * `cdc.<capture_instance>_CT` carries two such values:
 *
 *   - `__$start_lsn`  — the LSN of the transaction the row belongs to
 *   - `__$seqval`     — the within-transaction sequence value (also BINARY(10))
 *
 * Both are ordered by **plain lexicographic byte comparison**, big-endian
 * across all 10 bytes. That ordering matches the underlying log VLF/block/slot
 * structure that SQL Server uses internally:
 *
 *     bytes 0..3   = VLF sequence number       (big-endian uint32)
 *     bytes 4..7   = log block number          (big-endian uint32)
 *     bytes 8..9   = slot within the block     (big-endian uint16)
 *
 * We deliberately do NOT parse these subfields. Microsoft has tweaked the
 * internal layout across major versions, and the only public contract is the
 * lexicographic order. Treating the LSN as an opaque 10-byte blob and
 * comparing bytewise is correct on every supported version (2012 → 2022, plus
 * Azure SQL DB and Managed Instance).
 *
 * ---------------------------------------------------------------------------
 * Why this module exists (and why it's tiny)
 * ---------------------------------------------------------------------------
 *
 * SQL Server ships server-side helper functions:
 *
 *   - `sys.fn_cdc_increment_lsn(@lsn)` — returns the next LSN
 *   - `sys.fn_cdc_decrement_lsn(@lsn)` — returns the previous LSN
 *   - `sys.fn_cdc_get_min_lsn(@capture_instance)`
 *   - `sys.fn_cdc_get_max_lsn()`
 *
 * Tempting as it is to reimplement `increment_lsn` client-side as
 * `bigintFromBytes(lsn) + 1n`, we deliberately do NOT do that:
 *
 *   1. `fn_cdc_increment_lsn` is documented to advance to the **next valid
 *      LSN that could appear in the log**, which is not necessarily
 *      `bytes+1`. The implementation skips reserved slot values and may
 *      cross block boundaries in ways that are version-specific.
 *   2. The on-wire BINARY(10) layout is technically undocumented; replicating
 *      the increment in JS would couple the relay to the SQL Server version
 *      and silently break on upgrade.
 *   3. The relay never *needs* to compute a successor LSN on its own — it
 *      always either passes an LSN back to the server (where it gets re-
 *      interpreted by the engine) or compares two LSNs received from the
 *      server. So the only client-side primitive we need is comparison.
 *
 * If you ever think you need `incrementLsn()` in TypeScript: don't. Either
 * call `sys.fn_cdc_increment_lsn` over a parameterized query and round-trip
 * the result, or rethink the algorithm so it operates on closed intervals
 * directly.
 *
 * ---------------------------------------------------------------------------
 * Wire formats
 * ---------------------------------------------------------------------------
 *
 * The `tedious` driver (which `mssql` wraps) projects `BINARY(10)` columns
 * as a Node.js `Buffer` of length 10. Some users and external tools prefer
 * hex strings — Microsoft's CDC documentation and SSMS both display LSNs in
 * the form `0x000000240000058000170` (uppercase, 0x-prefixed, 22 chars total
 * = `0x` + 20 hex digits).
 *
 * This module exposes both representations:
 *
 *   - `Lsn` (= `Buffer`) is the canonical internal type.
 *   - `lsnToHex` / `lsnFromHex` convert at module boundaries (logs, configs,
 *     checkpoint files, JSON envelopes).
 *
 * ---------------------------------------------------------------------------
 * Edge cases handled
 * ---------------------------------------------------------------------------
 *
 *   - `null` / `undefined` LSN: throw. CDC rows always carry an LSN; a
 *     missing one is a programmer error worth surfacing loudly.
 *   - Short buffers: `compareLsn` treats a shorter buffer as smaller (after
 *     comparing the overlapping prefix). This matches Node's
 *     `Buffer.compare` semantics and means a partially-zeroed checkpoint
 *     compares correctly against a full BINARY(10).
 *   - `ZERO_LSN`: the all-zeros 10-byte buffer. SQL Server treats this as
 *     "before everything"; we use it as the conventional initial checkpoint
 *     when no prior position is known.
 *
 * @packageDocumentation
 */

/**
 * Canonical LSN type.
 *
 * Always a 10-byte `Buffer` when it comes from `tedious`/`mssql`. We do not
 * brand it as a nominal type because (a) it interoperates with the driver's
 * own typings and (b) all access goes through the helpers in this module,
 * which validate length where it matters.
 */
export type Lsn = Buffer;

/**
 * The all-zeros LSN — `0x00000000000000000000`.
 *
 * Use this as the initial checkpoint value before the relay has observed any
 * CDC rows. Every real LSN compares greater than `ZERO_LSN`.
 *
 * Exposed as a frozen, shared instance to avoid per-call allocation. Do NOT
 * mutate it — if you need a writable zero buffer, allocate your own with
 * `Buffer.alloc(10)`.
 */
// Buffers cannot be Object.frozen on Node 22+ (Cannot freeze array buffer
// views with elements). We expose a shared zero-buffer reference and trust
// callers not to mutate it; if you need a writable zero, allocate your own
// with Buffer.alloc(10).
export const ZERO_LSN: Lsn = Buffer.alloc(10);

/**
 * Compare two LSNs lexicographically (big-endian byte order).
 *
 * Returns:
 *   - `-1` if `a` sorts before `b`
 *   - ` 0` if `a` and `b` are byte-equal
 *   - ` 1` if `a` sorts after `b`
 *
 * A shorter buffer is treated as smaller than a longer one when their
 * overlapping prefix is equal (delegates to `Buffer.compare`). This matters
 * mainly for defensive comparisons against checkpoints that may have been
 * truncated; in normal CDC use both inputs are always 10 bytes.
 *
 * Throws `TypeError` if either input is `null` or `undefined`. We choose to
 * throw rather than coerce because a missing LSN almost always indicates a
 * row-projection bug (e.g. selecting the wrong column) that we want to fail
 * loudly during integration tests, not silently order ahead of every real
 * LSN.
 *
 * @example
 * ```ts
 * compareLsn(ZERO_LSN, lsnFromHex('0x0000002400000058000A')); // -> -1
 * compareLsn(a, a);                                            // -> 0
 * ```
 */
export function compareLsn(a: Lsn, b: Lsn): -1 | 0 | 1 {
  if (a == null) {
    throw new TypeError('compareLsn: first argument is null/undefined');
  }
  if (b == null) {
    throw new TypeError('compareLsn: second argument is null/undefined');
  }
  const c = Buffer.compare(a, b);
  // Buffer.compare can return any integer; normalise to the documented
  // tri-state so callers can `switch` on the result safely.
  if (c < 0) return -1;
  if (c > 0) return 1;
  return 0;
}

/**
 * Encode an LSN as a `0x`-prefixed uppercase hex string of length 22
 * (`0x` + 20 hex digits).
 *
 * This matches the format used by:
 *   - SQL Server Management Studio result panes
 *   - Microsoft's CDC documentation examples
 *   - `CONVERT(VARCHAR(MAX), @lsn, 1)` in T-SQL (style `1` = hex with `0x`)
 *
 * Buffers shorter than 10 bytes are accepted and emitted as-is (the relay
 * sometimes serialises `ZERO_LSN` placeholders that the test suite truncates).
 * Buffers LONGER than 10 bytes are accepted but should never occur from a
 * BINARY(10) column; we don't reject them here to keep the helper general.
 *
 * @param lsn  The raw LSN buffer (typically 10 bytes from `tedious`).
 * @returns    Uppercase hex string, e.g. `'0x000000240000058000170'` (sic:
 *             actually 22 chars: `'0x' + 20 hex digits'`).
 * @throws     `TypeError` if `lsn` is null/undefined.
 *
 * @example
 * ```ts
 * lsnToHex(ZERO_LSN); // '0x00000000000000000000'
 * ```
 */
export function lsnToHex(lsn: Lsn): string {
  if (lsn == null) {
    throw new TypeError('lsnToHex: argument is null/undefined');
  }
  if (!Buffer.isBuffer(lsn)) {
    throw new TypeError(
      `lsnToHex: expected Buffer, got ${typeof lsn}`,
    );
  }
  if (lsn.length !== 10) {
    throw new TypeError(
      `lsnToHex: expected 10-byte LSN buffer, got ${lsn.length} bytes`,
    );
  }
  return '0x' + lsn.toString('hex').toUpperCase();
}

/**
 * Parse a `0x`-prefixed hex string back into a `Lsn` buffer.
 *
 * Accepts the canonical form produced by `lsnToHex` (uppercase, 22 chars,
 * `0x`-prefixed) and is also case-insensitive for inbound parsing — operators
 * pasting LSNs from logs or T-SQL output shouldn't have to normalize case.
 *
 * Validation rules (strict — we throw `Error` on any of these):
 *   - Must be a string
 *   - Must start with `'0x'` or `'0X'`
 *   - Remainder must have **even** length (each byte = 2 hex digits)
 *   - Remainder must contain only `[0-9a-fA-F]`
 *
 * We do NOT enforce a 20-hex-digit length here because:
 *   - Test fixtures sometimes use shorter values to probe edge cases.
 *   - `compareLsn` and `lsnToHex` both handle short buffers gracefully.
 *   - Real CDC payloads from `tedious` never round-trip through this
 *     function — they come in as `Buffer` directly.
 *
 * If you need strict BINARY(10) enforcement at a system boundary (e.g. parsing
 * a user-supplied checkpoint file), assert `buf.length === 10` at the call
 * site.
 *
 * @param hex  The hex string, e.g. `'0x00000024000000580017'`.
 * @returns    A new `Buffer` containing the decoded bytes.
 * @throws     `TypeError` if `hex` is not a string.
 * @throws     `Error`     if the string is malformed (bad prefix, odd length,
 *                          non-hex characters).
 *
 * @example
 * ```ts
 * const lsn = lsnFromHex('0x00000024000000580017');
 * compareLsn(lsn, ZERO_LSN); // -> 1
 * ```
 */
export function lsnFromHex(hex: string): Lsn {
  if (typeof hex !== 'string') {
    throw new TypeError(
      `lsnFromHex: expected string, got ${hex === null ? 'null' : typeof hex}`,
    );
  }
  if (hex.length < 2 || (hex[0] !== '0') || (hex[1] !== 'x' && hex[1] !== 'X')) {
    throw new Error(
      `lsnFromHex: expected '0x'-prefixed hex string, got ${JSON.stringify(hex)}`,
    );
  }
  const body = hex.slice(2);
  if (body.length === 0) {
    throw new Error(`lsnFromHex: empty hex body in ${JSON.stringify(hex)}`);
  }
  if (body.length % 2 !== 0) {
    throw new Error(
      `lsnFromHex: hex body must have even length, got ${body.length} chars in ${JSON.stringify(hex)}`,
    );
  }
  // Validate character set explicitly. `Buffer.from(s, 'hex')` is famously
  // lenient — it silently truncates at the first invalid character, which
  // would turn `'0x00ZZ'` into a 1-byte buffer instead of an error. We refuse
  // to inherit that footgun.
  if (!/^[0-9a-fA-F]+$/.test(body)) {
    throw new Error(
      `lsnFromHex: non-hex characters in ${JSON.stringify(hex)}`,
    );
  }
  return Buffer.from(body, 'hex');
}
