import {
  DuplicateHeaderError,
  parseCsvRecords,
  RowLimitExceededError,
} from "../csv/parse-csv-records.js";
import {
  createImportBatch,
  failImportBatch,
  getImportSummary,
  type ImportSummary,
} from "../db/imports.js";
import {
  prepareRecordStaging,
  RecordStagingCallbackError,
} from "./prepare-record-staging.js";
import {
  persistRecordStaging,
} from "./persist-record-staging.js";
import {
  withDedicatedConnection,
  type PostgresDatabase,
  type PostgresQueryable,
} from "../db/postgres.js";
import type {
  RecordSchemaContract,
  StagingDiagnostic,
} from "./types.js";
import { UnsupportedRecordSchemaError } from "./validate-headers.js";
import { sourceContentMetadata } from "./source-provenance.js";
import {
  resolveIngestionLimits,
  type IngestionLimits,
} from "./limits.js";

export type RunRecordImportInput = {
  db: PostgresDatabase;
  importId: string;
  contract: RecordSchemaContract;
  csvText: string;
  transform: (row: Record<string, string>) => Record<string, unknown>;
  getRecordId: (
    row: Record<string, string>,
    canonical: Record<string, unknown>,
  ) => string | null;
  diagnose?: (
    row: Record<string, string>,
    canonical: Record<string, unknown>,
  ) => StagingDiagnostic[];
  limits?: Partial<IngestionLimits>;
};

type RunRecordImportOnConnectionInput = Omit<
  RunRecordImportInput,
  "db" | "limits"
> & {
  db: PostgresQueryable;
  limits: IngestionLimits;
};

export type RunRecordImportResult = {
  importId: string;
  status: "VALIDATED" | "FAILED";
  summary: ImportSummary;
};

function errorDetail(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

async function resultFromCommittedSummary(
  db: PostgresQueryable,
  importId: string,
  status: "VALIDATED" | "FAILED",
): Promise<RunRecordImportResult> {
  const summary = await getImportSummary(db, importId);
  if (summary === null) {
    throw new Error(`Import summary missing after terminalization: ${importId}`);
  }
  return { importId, status, summary };
}

async function bestEffortFail(
  db: PostgresQueryable,
  importId: string,
  issueCode: string,
  detail: string,
): Promise<void> {
  try {
    await failImportBatch(db, { importId, issueCode, detail });
  } catch {
    // Audit recovery is best-effort; the original failure remains authoritative.
  }
}

export async function runRecordImportAfterBatch(
  input: RunRecordImportOnConnectionInput,
): Promise<RunRecordImportResult> {
  let parsed: ReturnType<typeof parseCsvRecords>;
  try {
    parsed = parseCsvRecords(input.csvText, {
      maxDataRows: input.limits.maxDataRows,
    });
  } catch (error) {
    if (error instanceof DuplicateHeaderError) {
      await failImportBatch(input.db, {
        importId: input.importId,
        issueCode: "DUPLICATE_HEADER",
        detail: errorDetail(error),
      });
      return resultFromCommittedSummary(input.db, input.importId, "FAILED");
    }
    if (error instanceof RowLimitExceededError) {
      await failImportBatch(input.db, {
        importId: input.importId,
        issueCode: "ROW_LIMIT_EXCEEDED",
        detail: errorDetail(error),
      });
      return resultFromCommittedSummary(input.db, input.importId, "FAILED");
    }
    await failImportBatch(input.db, {
      importId: input.importId,
      issueCode: "CSV_PARSE_ERROR",
      detail: errorDetail(error),
    });
    return resultFromCommittedSummary(input.db, input.importId, "FAILED");
  }

  let prepared: ReturnType<typeof prepareRecordStaging>;
  try {
    prepared = prepareRecordStaging({
      contract: input.contract,
      headers: parsed.headers,
      rows: parsed.rows,
      transform: input.transform,
      getRecordId: input.getRecordId,
      diagnose: input.diagnose,
    });
  } catch (error) {
    if (error instanceof UnsupportedRecordSchemaError) {
      await failImportBatch(input.db, {
        importId: input.importId,
        issueCode: "SCHEMA_HEADER_ERROR",
        detail: errorDetail(error),
      });
      return resultFromCommittedSummary(input.db, input.importId, "FAILED");
    }
    if (error instanceof RecordStagingCallbackError) {
      await bestEffortFail(
        input.db,
        input.importId,
        "STAGING_CALLBACK_ERROR",
        errorDetail(error),
      );
      throw error.cause;
    }
    throw error;
  }

  let persisted: Awaited<ReturnType<typeof persistRecordStaging>>;
  try {
    persisted = await persistRecordStaging(input.db, {
      importId: input.importId,
      rows: prepared.rows,
    });
  } catch (error) {
    await bestEffortFail(
      input.db,
      input.importId,
      "IMPORT_PERSISTENCE_ERROR",
      errorDetail(error),
    );
    throw error;
  }

  return resultFromCommittedSummary(input.db, input.importId, persisted.status);
}

export async function runRecordImport(
  input: RunRecordImportInput,
): Promise<RunRecordImportResult> {
  const limits = resolveIngestionLimits(input.limits);
  return withDedicatedConnection(input.db, (client) =>
    runRecordImportOnConnection({ ...input, db: client, limits }));
}

async function runRecordImportOnConnection(
  input: RunRecordImportOnConnectionInput,
): Promise<RunRecordImportResult> {
  const sourceBytes = Buffer.from(input.csvText, "utf8");
  await createImportBatch(input.db, {
    importId: input.importId,
    schemaVersion: input.contract.schemaVersion,
    sourceKind: "CSV_TEXT",
    ...sourceContentMetadata(sourceBytes),
  });

  if (sourceBytes.length > input.limits.maxSourceBytes) {
    await failImportBatch(input.db, {
      importId: input.importId,
      issueCode: "SOURCE_SIZE_LIMIT_EXCEEDED",
      detail: `Source size ${sourceBytes.length} bytes exceeds maxSourceBytes ${input.limits.maxSourceBytes}.`,
    });
    return resultFromCommittedSummary(input.db, input.importId, "FAILED");
  }

  return runRecordImportAfterBatch(input);
}
