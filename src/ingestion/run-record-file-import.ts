import { readFile, stat } from "node:fs/promises";
import { basename } from "node:path";
import {
  createImportBatch,
  failImportBatch,
  getImportSummary,
  updateImportSourceContentMetadata,
} from "../db/imports.js";
import {
  withDedicatedConnection,
  type PostgresDatabase,
  type PostgresQueryable,
} from "../db/postgres.js";
import { sourceContentMetadata } from "./source-provenance.js";
import {
  runRecordImportAfterBatch,
  type RunRecordImportResult,
} from "./run-record-import.js";
import type {
  RecordSchemaContract,
  StagingDiagnostic,
} from "./types.js";
import {
  resolveIngestionLimits,
  type IngestionLimits,
} from "./limits.js";

type FileOperations = {
  stat(filePath: string): Promise<{ size: number }>;
  readFile(filePath: string): Promise<Buffer>;
};

const defaultFileOperations: FileOperations = { stat, readFile };

export type RunRecordFileImportInput = {
  db: PostgresDatabase;
  importId: string;
  contract: RecordSchemaContract;
  filePath: string;
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

type RunRecordFileImportOnConnectionInput = Omit<
  RunRecordFileImportInput,
  "db" | "limits"
> & { db: PostgresQueryable; limits: IngestionLimits };

function errorDetail(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

async function failedFileResult(
  input: RunRecordFileImportOnConnectionInput,
  issueCode: "FILE_READ_ERROR" | "SOURCE_SIZE_LIMIT_EXCEEDED",
  error: unknown,
): Promise<RunRecordImportResult> {
  await failImportBatch(input.db, {
    importId: input.importId,
    issueCode,
    detail: errorDetail(error),
  });

  const summary = await getImportSummary(input.db, input.importId);
  if (summary === null) {
    throw new Error(`Import summary missing after terminalization: ${input.importId}`);
  }

  return {
    importId: input.importId,
    status: "FAILED",
    summary,
  };
}

export async function runRecordFileImport(
  input: RunRecordFileImportInput,
): Promise<RunRecordImportResult> {
  return runRecordFileImportWithFileOperations(input, defaultFileOperations);
}

/** @internal Test-only seam for deterministic source-change scenarios. */
export async function runRecordFileImportWithFileOperationsForTest(
  input: RunRecordFileImportInput,
  fileOperations: FileOperations,
): Promise<RunRecordImportResult> {
  return runRecordFileImportWithFileOperations(input, fileOperations);
}

async function runRecordFileImportWithFileOperations(
  input: RunRecordFileImportInput,
  fileOperations: FileOperations,
): Promise<RunRecordImportResult> {
  const limits = resolveIngestionLimits(input.limits);
  return withDedicatedConnection(input.db, (client) =>
    runRecordFileImportOnConnection(
      { ...input, db: client, limits },
      fileOperations,
    ));
}

async function runRecordFileImportOnConnection(
  input: RunRecordFileImportOnConnectionInput,
  fileOperations: FileOperations,
): Promise<RunRecordImportResult> {
  await createImportBatch(input.db, {
    importId: input.importId,
    schemaVersion: input.contract.schemaVersion,
    sourceKind: "LOCAL_FILE",
    sourceName: basename(input.filePath),
  });

  let sourceSize: number;
  try {
    sourceSize = (await fileOperations.stat(input.filePath)).size;
  } catch (error) {
    return failedFileResult(input, "FILE_READ_ERROR", error);
  }

  if (sourceSize > input.limits.maxSourceBytes) {
    return failedFileResult(
      input,
      "SOURCE_SIZE_LIMIT_EXCEEDED",
      new Error(
        `Source size ${sourceSize} bytes exceeds maxSourceBytes ${input.limits.maxSourceBytes}.`,
      ),
    );
  }

  let bytes: Buffer;
  try {
    bytes = await fileOperations.readFile(input.filePath);
  } catch (error) {
    return failedFileResult(input, "FILE_READ_ERROR", error);
  }

  const contentMetadata = sourceContentMetadata(bytes);
  try {
    await updateImportSourceContentMetadata(input.db, {
      importId: input.importId,
      ...contentMetadata,
    });
  } catch (error) {
    try {
      await failImportBatch(input.db, {
        importId: input.importId,
        issueCode: "IMPORT_PROVENANCE_ERROR",
        detail: errorDetail(error),
      });
    } catch {
      // Recovery must never replace the original metadata persistence error.
    }
    throw error;
  }

  if (bytes.length > input.limits.maxSourceBytes) {
    return failedFileResult(
      input,
      "SOURCE_SIZE_LIMIT_EXCEEDED",
      new Error(
        `Source size ${bytes.length} bytes exceeds maxSourceBytes ${input.limits.maxSourceBytes}.`,
      ),
    );
  }

  let csvText: string;
  try {
    csvText = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch (error) {
    return failedFileResult(input, "FILE_READ_ERROR", error);
  }

  return runRecordImportAfterBatch({
    db: input.db,
    importId: input.importId,
    contract: input.contract,
    csvText,
    transform: input.transform,
    getRecordId: input.getRecordId,
    diagnose: input.diagnose,
    limits: input.limits,
  });
}
