import {
  getImportBatch,
  getImportSummary as readImportSummary,
  listImportRowsPage,
  listImportIssuesPage,
  type ImportBatch,
  type ImportSummary,
  type ImportRow,
  type ImportIssue,
  type ImportRowsPageOptions,
  type ImportIssuesPageOptions,
} from "./db/imports.js";
import { withDedicatedConnection, type PostgresDatabase } from "./db/postgres.js";
import type { Page } from "./db/pagination.js";
import type { MigrationResult } from "./db/migrations.js";
import type { IngestionLimits } from "./ingestion/limits.js";
import type { RecordSchemaContract, StagingDiagnostic } from "./ingestion/types.js";

export { FrameworkError } from "./errors.js";
export type { FrameworkErrorCode } from "./errors.js";
export { DEFAULT_INGESTION_LIMITS, MAX_INGESTION_LIMITS } from "./ingestion/limits.js";
export type { IngestionLimits } from "./ingestion/limits.js";
export { DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE } from "./db/pagination.js";
export type { Page } from "./db/pagination.js";
export type { PostgresDatabase, PostgresQueryable, PostgresPoolClient } from "./db/postgres.js";
export type {
  ImportBatch, ImportBatchStatus, ImportSourceKind, ImportSourceMetadata,
  ImportSummary, ImportRow, ImportRowStatus, ImportIssue, ImportIssueSeverity,
  ImportRowsPageOptions, ImportIssuesPageOptions,
} from "./db/imports.js";
export type { MigrationResult } from "./db/migrations.js";

export type ImportSource =
  | { kind: "CSV_TEXT"; text: string }
  | { kind: "LOCAL_FILE"; filePath: string };

export type DurableImportInput = {
  database: PostgresDatabase;
  importId: string;
  contract: RecordSchemaContract;
  source: ImportSource;
  limits?: Partial<IngestionLimits>;
  transform: (row: Record<string, string>) => Record<string, unknown>;
  getRecordId: (row: Record<string, string>, canonical: Record<string, unknown>) => string | null;
  diagnose?: (row: Record<string, string>, canonical: Record<string, unknown>) => StagingDiagnostic[];
};

export type RunRecordImportResult = {
  importId: string;
  status: "VALIDATED" | "FAILED";
  summary: ImportSummary;
};

export type RunMigrationsOptions = { migrationsDir?: string };

// Load CSV/filesystem workflows on demand so preparation-only consumers do not
// need runtime CSV dependencies or Node-specific types in their declarations.
export async function startDurableImport(input: DurableImportInput): Promise<RunRecordImportResult> {
  const workflow = await import("./ingestion/durable-import.js");
  return workflow.startDurableImport(input);
}

export async function resumeDurableImport(input: DurableImportInput): Promise<RunRecordImportResult> {
  const workflow = await import("./ingestion/durable-import.js");
  return workflow.resumeDurableImport(input);
}

export function getImportAttempt(database: PostgresDatabase, importId: string): Promise<ImportBatch | null> {
  return withDedicatedConnection(database, client => getImportBatch(client, importId));
}

export function getImportSummary(database: PostgresDatabase, importId: string): Promise<ImportSummary | null> {
  return withDedicatedConnection(database, client => readImportSummary(client, importId));
}

export function getImportRowsPage(database: PostgresDatabase, importId: string, options?: ImportRowsPageOptions): Promise<Page<ImportRow>> {
  return withDedicatedConnection(database, client => listImportRowsPage(client, importId, options));
}

export function getImportIssuesPage(database: PostgresDatabase, importId: string, options?: ImportIssuesPageOptions): Promise<Page<ImportIssue>> {
  return withDedicatedConnection(database, client => listImportIssuesPage(client, importId, options));
}

export async function runMigrations(database: PostgresDatabase, options?: RunMigrationsOptions): Promise<MigrationResult> {
  const migrations = await import("./db/migrations.js");
  return migrations.runMigrations(database, options);
}
