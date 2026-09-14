export const GENERIC_HEADERS = [
  "record_id",
  "first_name",
  "last_name",
  "status",
  "region",
  "legacy_history_code",
] as const;

export const GENERIC_CONTRACT = {
  schemaVersion: "GENERIC_V1",
  requiredHeaders: ["record_id", "first_name", "last_name", "status"],
  optionalHeaders: ["region", "legacy_history_code"],
} as const;

export function syntheticRecord(overrides: Record<string, string> = {}) {
  return {
    record_id: " R-001 ",
    first_name: " Ada ",
    last_name: " Example ",
    status: " active ",
    region: " north ",
    legacy_history_code: " RAW-7 ",
    ...overrides,
  };
}
