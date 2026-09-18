import { startDurableImport, type DurableImportInput, type DurableImportResult } from "./durable-import.js";
import type { PostgresDatabase } from "../db/postgres.js";
export type RunRecordImportInput = Omit<DurableImportInput, "database" | "source"> & { db: PostgresDatabase; csvText: string };
export type RunRecordImportResult = DurableImportResult;
export function runRecordImport(input: RunRecordImportInput): Promise<RunRecordImportResult> {
  const { db, csvText, ...rest } = input;
  return startDurableImport({ ...rest, database: db, source: { kind: "CSV_TEXT", text: csvText } });
}
