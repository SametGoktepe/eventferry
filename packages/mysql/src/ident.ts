/**
 * Allow only safe SQL identifier characters. Used wherever a user-supplied name
 * (table) is interpolated into SQL, to prevent injection.
 */
export function assertIdent(name: string): string {
  if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) {
    throw new Error(
      `Invalid identifier "${name}": must match /^[a-zA-Z_][a-zA-Z0-9_]*$/`,
    );
  }
  return name;
}
