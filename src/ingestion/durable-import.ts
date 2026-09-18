import { readFile, stat } from "node:fs/promises";
import { basename } from "node:path";
import { DuplicateHeaderError, parseCsvRecords, RowLimitExceededError } from "../csv/parse-csv-records.js";
import { createImportBatch, failImportBatch, getImportBatch, getImportSummary, updateImportSourceContentMetadata, completeImportSourceMetadata, assertImportProvenance, type ImportBatch, type ImportSummary } from "../db/imports.js";
import { FrameworkError } from "../errors.js";
import { prepareRecordStaging, RecordStagingCallbackError } from "./prepare-record-staging.js";
import { persistRecordStaging } from "./persist-record-staging.js";
import { withDedicatedConnection, type PostgresDatabase, type PostgresQueryable } from "../db/postgres.js";
import type { RecordSchemaContract, StagingDiagnostic } from "./types.js";
import { UnsupportedRecordSchemaError } from "./validate-headers.js";
import { sourceContentMetadata } from "./source-provenance.js";
import { resolveIngestionLimits, type IngestionLimits } from "./limits.js";

export type DurableImportSource = { kind: "CSV_TEXT"; text: string } | { kind: "LOCAL_FILE"; filePath: string };
export type DurableImportInput = {
  database: PostgresDatabase;
  importId: string;
  contract: RecordSchemaContract;
  source: DurableImportSource;
  transform: (row: Record<string, string>) => Record<string, unknown>;
  getRecordId: (row: Record<string, string>, canonical: Record<string, unknown>) => string | null;
  diagnose?: (row: Record<string, string>, canonical: Record<string, unknown>) => StagingDiagnostic[];
  limits?: Partial<IngestionLimits>;
};
export type DurableImportResult = { importId: string; status: "VALIDATED" | "FAILED"; summary: ImportSummary };
export type FileOperations = { stat(filePath: string): Promise<{ size: number }>; readFile(filePath: string): Promise<Buffer> };
type ConnectionInput = Omit<DurableImportInput, "database" | "limits"> & { db: PostgresQueryable; limits: IngestionLimits };
type RunRecordImportOnConnectionInput = ConnectionInput & { csvText: string };
type RunRecordImportResult = DurableImportResult;

export function startDurableImport(input: DurableImportInput): Promise<DurableImportResult> {
  return runDurableImport(input, "START");
}
export function resumeDurableImport(input: DurableImportInput): Promise<DurableImportResult> {
  return runDurableImport(input, "RESUME");
}
/** @internal Supports the legacy file adapter's deterministic filesystem tests. */
export function startDurableImportWithFileOperations(input: DurableImportInput, operations: FileOperations): Promise<DurableImportResult> {
  return runDurableImport(input, "START", operations);
}
async function runDurableImport(input: DurableImportInput, mode: "START" | "RESUME", operations: FileOperations = { stat, readFile }): Promise<DurableImportResult> {
  const limits = resolveIngestionLimits(input.limits);
  return withDedicatedConnection(input.database, db => runOnConnection({ ...input, db, limits }, mode, operations));
}
async function runOnConnection(input: ConnectionInput, mode: "START" | "RESUME", operations: FileOperations): Promise<DurableImportResult> {
  const { source } = input;
  const identity = { schemaVersion: input.contract.schemaVersion, sourceKind: source.kind, sourceName: source.kind === "LOCAL_FILE" ? basename(source.filePath) : null, sourcePath: source.kind === "LOCAL_FILE" ? source.filePath : null };
  let existing: ImportBatch | null = null;
  if (mode === "RESUME") {
    existing = await getImportBatch(input.db, input.importId);
    if (existing === null) throw new FrameworkError("IMPORT_NOT_FOUND", `Import batch not found: ${input.importId}`);
    if (existing.status !== "RECEIVED") throw new FrameworkError("IMPORT_NOT_RESUMABLE", "Import must be in RECEIVED status before resume.");
    assertImportProvenance(existing, identity);
  }
  let bytes: Buffer;
  if (source.kind === "CSV_TEXT") {
    bytes = Buffer.from(source.text, "utf8");
    if (mode === "START") await createImportBatch(input.db, { importId: input.importId, ...identity, sourcePath: null, ...sourceContentMetadata(bytes) });
  } else {
    if (mode === "START") await createImportBatch(input.db, { importId: input.importId, ...identity, sourcePath: null });
    let size: number;
    try { size = (await operations.stat(source.filePath)).size; }
    catch (error) { return failedSource(input, "FILE_READ_ERROR", errorDetail(error)); }
    // Established content size conflicts are rejected even when the resupply exceeds bounds.
    if (existing && size > input.limits.maxSourceBytes && existing.sourceSizeBytes !== null) assertImportProvenance(existing, { sourceSizeBytes: size });
    if (size > input.limits.maxSourceBytes) return failedSource(input, "SOURCE_SIZE_LIMIT_EXCEEDED", `Source size ${size} bytes exceeds maxSourceBytes ${input.limits.maxSourceBytes}.`);
    try { bytes = await operations.readFile(source.filePath); }
    catch (error) { return failedSource(input, "FILE_READ_ERROR", errorDetail(error)); }
  }
  const metadata = sourceContentMetadata(bytes);
  if (existing) {
    assertImportProvenance(existing, metadata);
    // The database repeats the comparison atomically before filling missing fields.
    // No completion may overwrite provenance established by a concurrent resume.
    await completeImportSourceMetadata(input.db, { importId: input.importId, ...identity, ...metadata });
  } else if (source.kind === "LOCAL_FILE") {
    try { await updateImportSourceContentMetadata(input.db, { importId: input.importId, ...metadata }); }
    catch (error) { await bestEffortFail(input.db, input.importId, "IMPORT_PROVENANCE_ERROR", errorDetail(error)); throw error; }
  }
  if (bytes.length > input.limits.maxSourceBytes) return failedSource(input, "SOURCE_SIZE_LIMIT_EXCEEDED", `Source size ${bytes.length} bytes exceeds maxSourceBytes ${input.limits.maxSourceBytes}.`);
  let csvText: string;
  if (source.kind === "CSV_TEXT") csvText = source.text;
  else {
    try { csvText = new TextDecoder("utf-8", { fatal: true }).decode(bytes); }
    catch (error) { return failedSource(input, "FILE_READ_ERROR", errorDetail(error)); }
  }
  return runRecordImportAfterBatch({ ...input, csvText });
}
async function failedSource(input: ConnectionInput, issueCode: string, detail: string): Promise<DurableImportResult> {
  await failImportBatch(input.db, { importId: input.importId, issueCode, detail });
  return resultFromCommittedSummary(input.db, input.importId, "FAILED");
}
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

async function runRecordImportAfterBatch(
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
