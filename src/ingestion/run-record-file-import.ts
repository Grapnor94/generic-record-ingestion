import { readFile } from "node:fs/promises";
import {
  createImportBatch,
  failImportBatch,
  getImportSummary,
} from "../db/imports.js";
import type { Queryable } from "./persist-record-staging.js";
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
  });

  let csvText: string;
  try {
    const bytes = await readFile(input.filePath);
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
