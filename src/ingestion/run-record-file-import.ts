import { readFile } from "node:fs/promises";
import { basename } from "node:path";
import {
  createImportBatch,
  failImportBatch,
  getImportSummary,
  updateImportSourceContentMetadata,
} from "../db/imports.js";
import type { Queryable } from "./persist-record-staging.js";
import { sourceContentMetadata } from "./source-provenance.js";
import {
  runRecordImportAfterBatch,
  type RunRecordImportResult,
} from "./run-record-import.js";
import type {
  RecordSchemaContract,
  StagingDiagnostic,
} from "./types.js";

export type RunRecordFileImportInput = {
  db: Queryable;
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
};

function errorDetail(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

async function failedFileResult(
  input: RunRecordFileImportInput,
  error: unknown,
): Promise<RunRecordImportResult> {
  await failImportBatch(input.db, {
    importId: input.importId,
    issueCode: "FILE_READ_ERROR",
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
  await createImportBatch(input.db, {
    importId: input.importId,
    schemaVersion: input.contract.schemaVersion,
    sourceKind: "LOCAL_FILE",
    sourceName: basename(input.filePath),
  });

  let bytes: Buffer;
  try {
    bytes = await readFile(input.filePath);
  } catch (error) {
    return failedFileResult(input, error);
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

  let csvText: string;
  try {
    csvText = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch (error) {
    return failedFileResult(input, error);
  }

  return runRecordImportAfterBatch({
    db: input.db,
    importId: input.importId,
    contract: input.contract,
    csvText,
    transform: input.transform,
    getRecordId: input.getRecordId,
    diagnose: input.diagnose,
  });
}
