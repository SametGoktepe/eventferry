/**
 * Resolve a {@link KafkaConnectionConfig}-style `transactionalId` into the
 * concrete string the underlying driver expects.
 *
 * Accepts:
 *   - `string`           — used verbatim.
 *   - `() => string`     — invoked once at connect time.
 *   - `() => Promise<string>` — awaited at connect time.
 *
 * Throws when the input is undefined (caller should pre-validate) or when
 * the callable yields an empty string.
 */
export async function resolveTransactionalId(
  input: string | (() => string | Promise<string>) | undefined,
): Promise<string> {
  if (input === undefined) {
    throw new Error("transactionalId is required when transactional=true");
  }
  const raw = typeof input === "function" ? await input() : input;
  if (typeof raw !== "string" || raw.length === 0) {
    throw new Error(
      "transactionalId resolver must return a non-empty string",
    );
  }
  return raw;
}
