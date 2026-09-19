import test from "node:test";
import { writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { createRequire } from "node:module";
import { run, withPackedConsumer } from "./packed-consumer.mjs";

const require = createRequire(import.meta.url);

test("packed root exposes only supported workflow values", async () => withPackedConsumer(async dir => {
  await writeFile(join(dir, "consumer.mjs"), `
import assert from "node:assert/strict";
import * as api from "generic-record-ingestion";
assert.deepEqual(Object.keys(api).sort(), [
  "prepareRecordStaging", "RecordStagingCallbackError", "UnsupportedRecordSchemaError",
  "startDurableImport", "resumeDurableImport", "getImportAttempt", "getImportSummary",
  "getImportRowsPage", "getImportIssuesPage", "runMigrations", "FrameworkError",
  "DEFAULT_INGESTION_LIMITS", "MAX_INGESTION_LIMITS", "DEFAULT_PAGE_SIZE", "MAX_PAGE_SIZE",
].sort());
const error = new api.FrameworkError("IMPORT_NOT_FOUND");
assert.ok(error instanceof Error);
assert.equal(error.code, "IMPORT_NOT_FOUND");
assert.ok(api.DEFAULT_INGESTION_LIMITS.maxDataRows <= api.MAX_INGESTION_LIMITS.maxDataRows);
assert.ok(api.DEFAULT_PAGE_SIZE <= api.MAX_PAGE_SIZE);
`);
  run(process.execPath, [join(dir, "consumer.mjs")], dir);
}));

test("packed package blocks internal repositories and declarations", async () => withPackedConsumer(async dir => {
  await writeFile(join(dir, "consumer.mjs"), `
import assert from "node:assert/strict";
for (const path of ["db/imports.js", "dist/db/imports.js", "dist/index.js", "package.json"]) {
  await assert.rejects(import("generic-record-ingestion/" + path), { code: "ERR_PACKAGE_PATH_NOT_EXPORTED" });
}
`);
  run(process.execPath, [join(dir, "consumer.mjs")], dir);
}));

test("packed TypeScript consumer resolves workflow, lifecycle, provenance and database contracts", async () => withPackedConsumer(async dir => {
  await writeFile(join(dir, "consumer.ts"), `
import {
  startDurableImport, resumeDurableImport, getImportAttempt, getImportSummary,
  getImportRowsPage, getImportIssuesPage, runMigrations, FrameworkError,
  type ImportSource, type DurableImportInput, type RunRecordImportResult,
  type IngestionLimits, type FrameworkErrorCode, type PostgresDatabase,
  type PostgresQueryable, type PostgresPoolClient, type ImportBatch,
  type ImportBatchStatus, type ImportSourceKind, type ImportSourceMetadata,
  type ImportSummary, type ImportRow, type ImportRowStatus, type ImportIssue,
  type ImportIssueSeverity, type ImportRowsPageOptions, type ImportIssuesPageOptions,
  type Page, type RunMigrationsOptions, type MigrationResult,
  type RecordSchemaContract, type StagingDiagnostic,
} from "generic-record-ingestion";
declare const client: PostgresQueryable;
declare const pooledClient: PostgresPoolClient;
const database: PostgresDatabase = { kind: "CLIENT", client };
const pool: PostgresDatabase = { kind: "POOL", pool: { connect: async () => pooledClient } };
const source: ImportSource = { kind: "CSV_TEXT", text: "record_id\\nR-1\\n" };
const file: ImportSource = { kind: "LOCAL_FILE", filePath: "/data/records.csv" };
const limits: IngestionLimits = { maxSourceBytes: 1000, maxDataRows: 10 };
const contract: RecordSchemaContract = { schemaVersion: "EXAMPLE_V1", requiredHeaders: ["record_id"] };
const input: DurableImportInput = { database, source, limits, importId: "one", contract,
  transform: row => ({ id: row.record_id }), getRecordId: row => row.record_id,
  diagnose: (): StagingDiagnostic[] => [] };
const started: Promise<RunRecordImportResult> = startDurableImport(input);
const resumed: Promise<RunRecordImportResult> = resumeDurableImport({ ...input, database: pool, source: file });
const attempt: Promise<ImportBatch | null> = getImportAttempt(database, "one");
const summary: Promise<ImportSummary | null> = getImportSummary(pool, "one");
const rowOptions: ImportRowsPageOptions = { pageSize: 1, status: "VALID" };
const issueOptions: ImportIssuesPageOptions = { pageSize: 1, severity: "WARNING", rowNumber: 1 };
const rows: Promise<Page<ImportRow>> = getImportRowsPage(database, "one", rowOptions);
const issues: Promise<Page<ImportIssue>> = getImportIssuesPage(pool, "one", issueOptions);
const options: RunMigrationsOptions = {};
const migrations: Promise<MigrationResult> = runMigrations(database, options);
const code: FrameworkErrorCode = new FrameworkError("IMPORT_NOT_FOUND").code;
const status: ImportBatchStatus = "RECEIVED";
const rowStatus: ImportRowStatus = "PENDING";
const severity: ImportIssueSeverity = "ERROR";
const sourceKind: ImportSourceKind = "CSV_TEXT";
declare const metadata: ImportSourceMetadata;
// @ts-expect-error invalid source discriminant
const badSource: ImportSource = { kind: "HTTP", text: "" };
// @ts-expect-error CLIENT requires a client
const badDatabase: PostgresDatabase = { kind: "CLIENT", pool: {} };
// @ts-expect-error low-level mutation is not a supported root export
import { createImportBatch } from "generic-record-ingestion";
// @ts-expect-error internal declarations are blocked by the exports map
import type { ImportBatch as InternalBatch } from "generic-record-ingestion/dist/db/imports.js";
void [started, resumed, attempt, summary, rows, issues, migrations, code, status, rowStatus, severity, sourceKind, metadata, badSource, badDatabase];
`);
  await writeFile(join(dir, "tsconfig.json"), JSON.stringify({ compilerOptions: {
    target: "ES2022", module: "NodeNext", moduleResolution: "NodeNext", strict: true,
    noEmit: true, types: [], skipLibCheck: false,
  }, files: ["consumer.ts"] }));
  run(process.execPath, [resolve(require.resolve("typescript"), "../../bin/tsc"), "-p", join(dir, "tsconfig.json")], dir);
}));
