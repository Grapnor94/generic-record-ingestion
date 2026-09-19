import { FrameworkError } from "../errors.js";

export type IngestionLimits = {
  maxSourceBytes: number;
  maxDataRows: number;
};

// Selected from the measured scenarios in docs/v0.8-bounded-operating-envelope.md.
export const DEFAULT_INGESTION_LIMITS: Readonly<IngestionLimits> = Object.freeze({
  maxSourceBytes: 20_074_811,
  maxDataRows: 25_000,
});

export const MAX_INGESTION_LIMITS: Readonly<IngestionLimits> = Object.freeze({
  maxSourceBytes: 80_298_686,
  maxDataRows: 100_000,
});

export function resolveIngestionLimits(partial: Partial<IngestionLimits> = {}): IngestionLimits {
  const limits = {
    maxSourceBytes: partial.maxSourceBytes === undefined ? DEFAULT_INGESTION_LIMITS.maxSourceBytes : partial.maxSourceBytes,
    maxDataRows: partial.maxDataRows === undefined ? DEFAULT_INGESTION_LIMITS.maxDataRows : partial.maxDataRows,
  };
  for (const field of ["maxSourceBytes", "maxDataRows"] as const) {
    const value = limits[field];
    if (!Number.isSafeInteger(value) || value <= 0 || value > MAX_INGESTION_LIMITS[field]) {
      throw new FrameworkError(
        "INVALID_INGESTION_LIMIT",
        `${field} must be a positive safe integer no greater than ${MAX_INGESTION_LIMITS[field]}.`,
        { details: { field, value, maximum: MAX_INGESTION_LIMITS[field] } },
      );
    }
  }
  return limits;
}
