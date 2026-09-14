import { parseCsvRecords } from "../csv/parse-csv-records.js";
import {
  createImportBatch,
  getImportSummary,
  type ImportSummary,
} from "../db/imports.js";
import { prepareRecordStaging } from "./prepare-record-staging.js";
import {
  persistRecordStaging,
  type Queryable,
} from "./persist-record-staging.js";
import type {
  RecordSchemaContract,
  StagingDiagnostic,
} from "./types.js";

export type RunRecordImportInput = {
  db: Queryable;
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
};

export type RunRecordImportResult = {
  importId: string;
  status: "VALIDATED" | "FAILED";
  summary: ImportSummary;
};

export async function runRecordImport(
  input: RunRecordImportInput,
): Promise<RunRecordImportResult> {
  await createImportBatch(input.db, {
    importId: input.importId,
    schemaVersion: input.contract.schemaVersion,
  });

  const parsed = parseCsvRecords(input.csvText);
  const prepared = prepareRecordStaging({
    contract: input.contract,
    headers: parsed.headers,
    rows: parsed.rows,
    transform: input.transform,
    getRecordId: input.getRecordId,
    diagnose: input.diagnose,
  });

  const persisted = await persistRecordStaging(input.db, {
    importId: input.importId,
    rows: prepared.rows,
  });

  const summary = await getImportSummary(input.db, input.importId);
  if (summary === null) {
    throw new Error(`Import summary missing after terminalization: ${input.importId}`);
  }

  return {
    importId: input.importId,
    status: persisted.status,
    summary,
  };
}
