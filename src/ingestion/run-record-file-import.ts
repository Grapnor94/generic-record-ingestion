import { startDurableImport, startDurableImportWithFileOperations, type DurableImportInput, type DurableImportResult, type FileOperations } from "./durable-import.js";
import type { PostgresDatabase } from "../db/postgres.js";
export type RunRecordFileImportInput = Omit<DurableImportInput, "database" | "source"> & { db: PostgresDatabase; filePath: string };
function durableInput(input: RunRecordFileImportInput): DurableImportInput {
  const { db, filePath, ...rest } = input;
  return { ...rest, database: db, source: { kind: "LOCAL_FILE", filePath } };
}
export function runRecordFileImport(input: RunRecordFileImportInput): Promise<DurableImportResult> {
  return startDurableImport(durableInput(input));
}
/** @internal Test-only seam for deterministic source-change scenarios. */
export function runRecordFileImportWithFileOperationsForTest(input: RunRecordFileImportInput, operations: FileOperations): Promise<DurableImportResult> {
  return startDurableImportWithFileOperations(durableInput(input), operations);
}
