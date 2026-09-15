import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { runRecordFileImport } from "../../dist/ingestion/run-record-file-import.js";

class MemoryImportDb {
  constructor() {
    this.batches = new Map();
    this.stage = [];
    this.issues = [];
    this.snapshot = null;
    this.nextIssueId = 1;
    this.stageInsertError = null;
    this.batchIssueError = null;
  }

  clone() {
    return {
      batches: new Map([...this.batches].map(([key, value]) => [key, { ...value }])),
      stage: structuredClone(this.stage),
      issues: structuredClone(this.issues),
      nextIssueId: this.nextIssueId,
    };
  }

  async query(sql, values = []) {
    const q = sql.replace(/\s+/g, " ").trim().toLowerCase();
    if (q === "begin") {
      this.snapshot = this.clone();
      return { rowCount: null, rows: [] };
    }
    if (q === "commit") {
      this.snapshot = null;
      return { rowCount: null, rows: [] };
    }
    if (q === "rollback") {
      if (this.snapshot) {
        this.batches = this.snapshot.batches;
        this.stage = this.snapshot.stage;
        this.issues = this.snapshot.issues;
        this.nextIssueId = this.snapshot.nextIssueId;
      }
      this.snapshot = null;
      return { rowCount: null, rows: [] };
    }
    if (q.startsWith("insert into import_batch")) {
      const [importId, schemaVersion] = values;
      if (this.batches.has(importId)) throw Object.assign(new Error("duplicate"), { code: "23505" });
      const row = { import_id: importId, schema_version: schemaVersion, status: "RECEIVED", created_at: new Date(), updated_at: new Date() };
      this.batches.set(importId, { ...row });
      return { rowCount: 1, rows: [row] };
    }
    if (q.startsWith("update import_batch") && q.includes("set status = 'failed'") && q.includes("status = 'received'")) {
      const batch = this.batches.get(values[0]);
      if (!batch || batch.status !== "RECEIVED") return { rowCount: 0, rows: [] };
      batch.status = "FAILED";
      return { rowCount: 1, rows: [{ import_id: values[0] }] };
    }
    if (q.startsWith("update import_batch") && q.includes("validating")) {
      const batch = this.batches.get(values[0]);
      if (!batch || batch.status !== "RECEIVED") return { rowCount: 0, rows: [] };
      batch.status = "VALIDATING";
      return { rowCount: 1, rows: [{ import_id: values[0] }] };
    }
    if (q.startsWith("insert into import_stage_row")) {
      if (this.stageInsertError) throw this.stageInsertError;
      const [importId, rowNumber, recordId, sourceJson, rawJson] = values;
      this.stage.push({ import_id: importId, row_number: rowNumber, record_id: recordId, source_row: JSON.parse(sourceJson), raw_source_row: rawJson === null ? null : JSON.parse(rawJson), validation_status: "PENDING" });
      return { rowCount: 1, rows: [] };
    }
    if (q.startsWith("insert into import_issue")) {
      if (values.length === 3) {
        if (this.batchIssueError) throw this.batchIssueError;
        const [importId, issueCode, detail] = values;
        this.issues.push({ issue_id: this.nextIssueId++, import_id: importId, row_number: null, record_id: null, issue_code: issueCode, severity: "ERROR", field_key: null, detail });
      } else {
        const [importId, rowNumber, recordId, issueCode, severity, fieldKey, detail] = values;
        this.issues.push({ issue_id: this.nextIssueId++, import_id: importId, row_number: rowNumber, record_id: recordId, issue_code: issueCode, severity, field_key: fieldKey, detail });
      }
      return { rowCount: 1, rows: [] };
    }
    if (q.startsWith("update import_stage_row")) {
      const [importId, invalidRows] = values;
      let count = 0;
      for (const row of this.stage) {
        if (row.import_id === importId) {
          row.validation_status = invalidRows.includes(row.row_number) ? "INVALID" : "VALID";
          count += 1;
        }
      }
      return { rowCount: count, rows: [] };
    }
    if (q.startsWith("update import_batch set status")) {
      const batch = this.batches.get(values[0]);
      if (!batch) return { rowCount: 0, rows: [] };
      batch.status = values[1];
      return { rowCount: 1, rows: [] };
    }
    if (q.startsWith("select") && q.includes("from import_batch b") && q.includes("left join lateral")) {
      const importId = values[0];
      const batch = this.batches.get(importId);
      if (!batch) return { rowCount: 0, rows: [] };
      const rows = this.stage.filter((row) => row.import_id === importId);
      const issues = this.issues.filter((issue) => issue.import_id === importId);
      return { rowCount: 1, rows: [{ import_id: importId, schema_version: batch.schema_version, status: batch.status, row_count: rows.length, valid_row_count: rows.filter((r) => r.validation_status === "VALID").length, invalid_row_count: rows.filter((r) => r.validation_status === "INVALID").length, pending_row_count: rows.filter((r) => r.validation_status === "PENDING").length, error_count: issues.filter((i) => i.severity === "ERROR").length, warning_count: issues.filter((i) => i.severity === "WARNING").length }] };
    }
    throw new Error(`Unsupported SQL: ${q}`);
  }
}

const contract = { schemaVersion: "v1", requiredHeaders: ["id", "name"] };
const transform = (row) => ({ name: row.name });
const getRecordId = (row) => row.id || null;

async function withTempDir(fn) {
  const dir = await mkdtemp(join(tmpdir(), "record-file-import-"));
  try { return await fn(dir); } finally { await rm(dir, { recursive: true, force: true }); }
}

async function csvFile(dir, contents, name = "records.csv") {
  const filePath = join(dir, name);
  await writeFile(filePath, contents, "utf8");
  return filePath;
}

function input(db, importId, filePath) {
  return { db, importId, contract, filePath, transform, getRecordId };
}

test("valid UTF-8 CSV file imports through the existing staging pipeline", async () => withTempDir(async (dir) => {
  const filePath = await csvFile(dir, "id,name\n1,Alice\n");
  const db = new MemoryImportDb();
  const result = await runRecordFileImport(input(db, "file-ok", filePath));
  assert.equal(result.status, "VALIDATED");
  assert.equal(result.summary.rowCount, 1);
  assert.equal(db.stage.length, 1);
  assert.deepEqual(db.stage[0].raw_source_row, { id: "1", name: "Alice" });
  assert.deepEqual(db.stage[0].source_row, { name: "Alice" });
}));

test("missing file becomes durable FILE_READ_ERROR with no staged rows", async () => withTempDir(async (dir) => {
  const db = new MemoryImportDb();
  const result = await runRecordFileImport(input(db, "file-missing", join(dir, "missing.csv")));
  assert.equal(result.status, "FAILED");
  assert.equal(result.summary.errorCount, 1);
  assert.equal(db.stage.length, 0);
  assert.equal(db.issues.length, 1);
  assert.equal(db.issues[0].issue_code, "FILE_READ_ERROR");
  assert.equal(db.issues[0].row_number, null);
  assert.equal(db.issues[0].record_id, null);
}));

test("malformed UTF-8 becomes durable FILE_READ_ERROR with no staged rows", async () => withTempDir(async (dir) => {
  const filePath = join(dir, "invalid.csv");
  await writeFile(filePath, Buffer.from([0x69, 0x64, 0x2c, 0x6e, 0x61, 0x6d, 0x65, 0x0a, 0x31, 0x2c, 0xc3, 0x28]));
  const db = new MemoryImportDb();
  const result = await runRecordFileImport(input(db, "file-invalid-utf8", filePath));
  assert.equal(result.status, "FAILED");
  assert.equal(result.summary.errorCount, 1);
  assert.equal(db.stage.length, 0);
  assert.equal(db.issues[0].issue_code, "FILE_READ_ERROR");
}));

test("malformed CSV preserves CSV_PARSE_ERROR semantics", async () => withTempDir(async (dir) => {
  const filePath = await csvFile(dir, 'id,name\n1,"Alice\n');
  const db = new MemoryImportDb();
  const result = await runRecordFileImport(input(db, "file-csv-bad", filePath));
  assert.equal(result.status, "FAILED");
  assert.equal(result.summary.rowCount, 0);
  assert.equal(db.stage.length, 0);
  assert.equal(db.issues[0].issue_code, "CSV_PARSE_ERROR");
}));

test("invalid headers preserve SCHEMA_HEADER_ERROR semantics", async () => withTempDir(async (dir) => {
  const filePath = await csvFile(dir, "id,unexpected\n1,Alice\n");
  const db = new MemoryImportDb();
  const result = await runRecordFileImport(input(db, "file-schema-bad", filePath));
  assert.equal(result.status, "FAILED");
  assert.equal(result.summary.rowCount, 0);
  assert.equal(db.issues[0].issue_code, "SCHEMA_HEADER_ERROR");
}));

test("row-level ERROR diagnostics persist invalid rows and return FAILED", async () => withTempDir(async (dir) => {
  const filePath = await csvFile(dir, "id,name\n1,Alice\n");
  const db = new MemoryImportDb();
  const result = await runRecordFileImport({
    ...input(db, "file-row-error", filePath),
    diagnose: () => [{ code: "BAD_NAME", severity: "ERROR", fieldKey: "name", detail: "Name rejected." }],
  });
  assert.equal(result.status, "FAILED");
  assert.equal(result.summary.invalidRowCount, 1);
  assert.equal(db.stage[0].validation_status, "INVALID");
  assert.equal(db.issues[0].issue_code, "BAD_NAME");
}));

test("warnings-only file imports return VALIDATED", async () => withTempDir(async (dir) => {
  const filePath = await csvFile(dir, "id,name\n1,Alice\n");
  const db = new MemoryImportDb();
  const result = await runRecordFileImport({
    ...input(db, "file-warning", filePath),
    diagnose: () => [{ code: "NAME_WARNING", severity: "WARNING", fieldKey: "name", detail: "Name retained." }],
  });
  assert.equal(result.status, "VALIDATED");
  assert.equal(result.summary.warningCount, 1);
  assert.equal(result.summary.errorCount, 0);
  assert.equal(db.stage[0].validation_status, "VALID");
}));

test("callback exceptions preserve exact original error identity and terminalize best-effort", async () => withTempDir(async (dir) => {
  const filePath = await csvFile(dir, "id,name\n1,Alice\n");
  const db = new MemoryImportDb();
  const original = new Error("transform exploded");
  await assert.rejects(
    () => runRecordFileImport({ ...input(db, "file-callback", filePath), transform: () => { throw original; } }),
    (error) => error === original,
  );
  assert.equal(db.batches.get("file-callback").status, "FAILED");
  assert.equal(db.stage.length, 0);
  assert.equal(db.issues[0].issue_code, "STAGING_CALLBACK_ERROR");
}));

test("persistence failures preserve exact original database error and terminalize best-effort", async () => withTempDir(async (dir) => {
  const filePath = await csvFile(dir, "id,name\n1,Alice\n");
  const db = new MemoryImportDb();
  const original = new Error("stage insert failed");
  db.stageInsertError = original;
  await assert.rejects(
    () => runRecordFileImport(input(db, "file-persist-fail", filePath)),
    (error) => error === original,
  );
  assert.equal(db.batches.get("file-persist-fail").status, "FAILED");
  assert.equal(db.stage.length, 0);
  assert.equal(db.issues[0].issue_code, "IMPORT_PERSISTENCE_ERROR");
}));

test("recovery failure does not replace the original persistence error", async () => withTempDir(async (dir) => {
  const filePath = await csvFile(dir, "id,name\n1,Alice\n");
  const db = new MemoryImportDb();
  const original = new Error("stage insert failed");
  db.stageInsertError = original;
  db.batchIssueError = new Error("recovery issue insert failed");
  await assert.rejects(
    () => runRecordFileImport(input(db, "file-recovery-fail", filePath)),
    (error) => error === original,
  );
  assert.equal(db.batches.get("file-recovery-fail").status, "RECEIVED");
  assert.equal(db.issues.length, 0);
}));

test("duplicate import IDs preserve the stable duplicate exception", async () => withTempDir(async (dir) => {
  const filePath = await csvFile(dir, "id,name\n1,Alice\n");
  const db = new MemoryImportDb();
  db.batches.set("file-duplicate", { import_id: "file-duplicate", schema_version: "v1", status: "RECEIVED" });
  await assert.rejects(
    () => runRecordFileImport(input(db, "file-duplicate", filePath)),
    { message: "Import batch already exists: file-duplicate" },
  );
  assert.equal(db.stage.length, 0);
}));