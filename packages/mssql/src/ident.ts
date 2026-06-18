/**
 * Allow only safe SQL identifier characters. Used wherever a user-supplied name
 * (schema, table) is interpolated into SQL, to prevent injection.
 *
 * The 100-character cap is engine-agnostic but also keeps embedded constraint
 * names like `CK_<table>_payload_json` within SQL Server's 128-char object
 * name limit. `label` ("schema", "table", ...) is woven into the error so the
 * caller knows which input failed validation.
 *
 * Called from the `MssqlStore` constructor AND from each top-level migration
 * export (`createMigrationSql`, `createRetentionIndexSql`) — every entrypoint
 * that interpolates an identifier must validate it.
 */
export function assertIdent(identifier: string, label: string): string {
  if (!/^[a-zA-Z_][a-zA-Z0-9_]{0,99}$/.test(identifier)) {
    throw new TypeError(
      `invalid SQL identifier '${identifier}' for ${label}: must match /^[a-zA-Z_][a-zA-Z0-9_]{0,99}$/`,
    );
  }
  return identifier;
}
