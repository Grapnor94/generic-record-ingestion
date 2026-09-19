export type FrameworkErrorCode =
  | "INVALID_INGESTION_LIMIT"
  | "INVALID_PAGE_SIZE"
  | "INVALID_CURSOR"
  | "IMPORT_ALREADY_EXISTS"
  | "IMPORT_NOT_FOUND"
  | "IMPORT_NOT_RESUMABLE"
  | "SOURCE_PROVENANCE_MISMATCH"
  | "MIGRATION_CHECKSUM_MISMATCH";

export class FrameworkError extends Error {
  readonly code: FrameworkErrorCode;
  readonly details?: Readonly<Record<string, unknown>>;
  declare readonly cause?: unknown;

  constructor(
    code: FrameworkErrorCode,
    message: string = code,
    options: {
      details?: Readonly<Record<string, unknown>>;
      cause?: unknown;
    } = {},
  ) {
    super(message, "cause" in options ? { cause: options.cause } : undefined);
    this.name = "FrameworkError";
    this.code = code;
    this.details = options.details;
  }
}
