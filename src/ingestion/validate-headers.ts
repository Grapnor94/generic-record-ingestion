import type { RecordSchemaContract } from "./types.js";

export class UnsupportedRecordSchemaError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "UnsupportedRecordSchemaError";
  }
}

export function assertSupportedRecordHeaders(
  contract: RecordSchemaContract,
  headers: readonly string[],
): void {
  const actual = new Set(headers);
  const allowed = new Set([
    ...contract.requiredHeaders,
    ...(contract.optionalHeaders ?? []),
  ]);

  const missing = contract.requiredHeaders.filter((header) => !actual.has(header));
  const unknown = headers.filter((header) => !allowed.has(header));

  if (missing.length === 0 && unknown.length === 0) return;

  const problems: string[] = [];
  if (missing.length > 0) problems.push(`missing required columns: ${missing.join(", ")}`);
  if (unknown.length > 0) problems.push(`unknown columns: ${unknown.join(", ")}`);

  throw new UnsupportedRecordSchemaError(
    `Record schema ${contract.schemaVersion} is not supported (${problems.join("; ")}).`,
  );
}
