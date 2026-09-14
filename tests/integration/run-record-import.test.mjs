import test from "node:test";
import assert from "node:assert/strict";
import { runRecordImport } from "../../dist/ingestion/run-record-import.js";

class MemoryImportDb {
  constructor() {
    this.batches = new Map();
    this.stage = [];
    this.issues = [];
    this.snapshot = null;
    this.nextIssueId = 1;
    this.stageInsertError = null;
    this.batchIssueError = null;
    this.returnMissingSummary = false;
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
      if (this.batches.has(importId)) {
        throw Object.assign(new Error("duplicate"), { code: "23505" });
      }
      const row = {
        import_id: importId,
        schema_version: schemaVersion,
        status: "RECEIVED",
        created_at: new Date("2026-09-14T00:00:00Z"),
        updated_at: new Date("2026-09-14T00:00:00Z"),
      };
      this.batches.set(importId, { ...row });
      return { rowCount: 1, rows: [row] };
    }

    if (
      q.startsWith("update import_batch") &&
      q.includes("set status = 'failed'") &&
      q.includes("status = 'received'")
    ) {
      const [importId] = values;
      const batch = this.batches.get(importId);
      if (!batch || batch.status !== "RECEIVED") return { rowCount: 0, rows: [] };
      batch.status = "FAILED";
      return { rowCount: 1, rows: [{ import_id: importId }] };
    }

    if (q.startsWith("update import_batch") && q.includes("validating")) {
      const [importId] = values;
      const batch = this.batches.get(importId);
      if (!batch || batch.status !== "RECEIVED") return { rowCount: 0, rows: [] };
      batch.status = "VALIDATING";
      return { rowCount: 1, rows: [{ import_id: importId }] };
    }

    if (q.startsWith("insert into import_stage_row")) {
      if (this.stageInsertError) throw this.stageInsertError;
      const [importId, rowNumber, recordId, sourceJson, rawJson] = values;
      this.stage.push({
        import_id: importId,
        row_number: rowNumber,
        record_id: recordId,
        source_row: JSON.parse(sourceJson),
        raw_source_row: rawJson === null ? null : JSON.parse(rawJson),
        validation_status: "PENDING",
      });
      return { rowCount: 1, rows: [] };
    }

    if (q.startsWith("insert into import_issue")) {
      if (values.length === 3) {
        if (this.batchIssueError) throw this.batchIssueError;
        const [importId, issueCode, detail] = values;
        this.issues.push({
          issue_id: this.nextIssueId++,
          import_id: importId,
          row_number: null,
          record_id: null,
          issue_code: issueCode,
          severity: "ERROR",
          field_key: null,
          detail,
        });
      } else {
        const [importId, rowNumber, recordId, issueCode, severity, fieldKey, detail] = values;
        this.issues.push({
          issue_id: this.nextIssueId++,
          import_id: importId,
          row_number: rowNumber,
          record_id: recordId,
          issue_code: issueCode,
          severity,
          field_key: fieldKey,
          detail,
        });
      }
      return { rowCount: 1, rows: [] };
    }

    if (q.startsWith("update import_stage_row")) {
      const [importId, invalidRows] = values;
      let count = 0;
      for (const row of this.stage) {
        if (row.import_id !== importId) continue;
        row.validation_status = invalidRows.includes(row.row_number) ? "INVALID" : "VALID";
        count += 1;
      }
      return { rowCount: count, rows: [] };
    }

    if (q.startsWith("update import_batch set status")) {
      const [importId, status] = values;
      const batch = this.batches.get(importId);
      if (!batch) return { rowCount: 0, rows: [] };
      batch.status = status;
      return { rowCount: 1, rows: [] };
    }

    if (q.startsWith("select") && q.includes("from import_batch b") && q.includes("left join lateral")) {
      if (this.returnMissingSummary) return { rowCount: 0, rows: [] };
      const [importId] = values;
      const batch = this.batches.get(importId);
      if (!batch) return { rowCount: 0, rows: [] };
      const rows = this.stage.filter((row) => row.import_id === importId);
      const issues = this.issues.filter((issue) => issue.import_id === importId);
      return {
        rowCount: 1,
        rows: [{
          import_id: importId,
          schema_version: batch.schema_version,
          status: batch.status,
          row_count: rows.length,
          valid_row_count: rows.filter((row) => row.validation_status === "VALID").length,
          invalid_row_count: rows.filter((row) => row.validation_status === "INVALID").length,
          pending_row_count: rows.filter((row) => row.validation_status === "PENDING").length,
          error_count: issues.filter((issue) => issue.severity === "ERROR").length,
          warning_count: issues.filter((issue) => issue.severity === "WARNING").length,
        }],
      };
    }

    throw new Error(`Unsupported SQL: ${q}`);
  }
}

const contract = {
  schemaVersion: "v1",
  requiredHeaders: ["id", "name"],
};

const transform = (row) => ({ name: row.name });
const getRecordId = (row) => row.id || null;

function baseInput(db, importId) {
  return {
    db,
    importId,
    contract,
    csvText: "id,name\n1,Alice\n",
    transform,
    getRecordId,
  };
}

test("runRecordImport persists a valid CSV import and returns the committed summary", async () => {
  const db = new MemoryImportDb();
  const result = await runRecordImport({
    ...baseInput(db, "import-ok"),
    csvText: "id,name\n1,Alice\n2,Bob\n",
  });

  assert.deepEqual(result, {
    importId: "import-ok",
    status: "VALIDATED",
    summary: {
      importId: "import-ok",
      schemaVersion: "v1",
      status: "VALIDATED",
      rowCount: 2,
      validRowCount: 2,
      invalidRowCount: 0,
      pendingRowCount: 0,
      errorCount: 0,
      warningCount: 0,
    },
  });
  assert.equal(db.stage.length, 2);
  assert.deepEqual(db.stage[0].raw_source_row, { id: "1", name: "Alice" });
  assert.deepEqual(db.stage[0].source_row, { name: "Alice" });
});

test("malformed CSV becomes a durable batch-level CSV_PARSE_ERROR", async () => {
  const db = new MemoryImportDb();
  const result = await runRecordImport({
    ...baseInput(db, "import-csv-bad"),
    csvText: 'id,name\n1,"Alice\n',
  });

  assert.equal(result.status, "FAILED");
  assert.equal(result.summary.status, "FAILED");
  assert.equal(result.summary.rowCount, 0);
  assert.equal(result.summary.errorCount, 1);
  assert.equal(db.stage.length, 0);
  assert.equal(db.issues.length, 1);
  assert.equal(db.issues[0].issue_code, "CSV_PARSE_ERROR");
  assert.equal(db.issues[0].row_number, null);
  assert.equal(db.issues[0].record_id, null);
});

test("unsupported schema headers become SCHEMA_HEADER_ERROR without stage rows", async () => {
  const db = new MemoryImportDb();
  const result = await runRecordImport({
    ...baseInput(db, "import-schema-bad"),
    csvText: "id,unexpected\n1,Alice\n",
  });

  assert.equal(result.status, "FAILED");
  assert.equal(result.summary.errorCount, 1);
  assert.equal(db.stage.length, 0);
  assert.equal(db.issues[0].issue_code, "SCHEMA_HEADER_ERROR");
  assert.equal(db.issues[0].row_number, null);
  assert.equal(db.issues[0].record_id, null);
});

test("row-level ERROR diagnostics persist and return FAILED without throwing", async () => {
  const db = new MemoryImportDb();
  const result = await runRecordImport({
    ...baseInput(db, "import-row-error"),
    diagnose: () => [{
      code: "BAD_NAME",
      severity: "ERROR",
      fieldKey: "name",
      detail: "Name rejected.",
    }],
  });

  assert.equal(result.status, "FAILED");
  assert.equal(result.summary.invalidRowCount, 1);
  assert.equal(result.summary.errorCount, 1);
  assert.equal(db.stage[0].validation_status, "INVALID");
  assert.equal(db.issues[0].issue_code, "BAD_NAME");
});

test("warnings alone persist and return VALIDATED", async () => {
  const db = new MemoryImportDb();
  const result = await runRecordImport({
    ...baseInput(db, "import-warning"),
    diagnose: () => [{
      code: "NAME_WARNING",
      severity: "WARNING",
      fieldKey: "name",
      detail: "Name retained with warning.",
    }],
  });

  assert.equal(result.status, "VALIDATED");
  assert.equal(result.summary.validRowCount, 1);
  assert.equal(result.summary.errorCount, 0);
  assert.equal(result.summary.warningCount, 1);
  assert.equal(db.stage[0].validation_status, "VALID");
});

test("callback exceptions are terminalized best-effort and rethrow the original error", async () => {
  const db = new MemoryImportDb();
  const original = new Error("transform exploded");

  await assert.rejects(
    () => runRecordImport({
      ...baseInput(db, "import-callback"),
      transform: () => { throw original; },
    }),
    (error) => error === original,
  );

  assert.equal(db.batches.get("import-callback").status, "FAILED");
  assert.equal(db.stage.length, 0);
  assert.equal(db.issues.length, 1);
  assert.equal(db.issues[0].issue_code, "STAGING_CALLBACK_ERROR");
  assert.equal(db.issues[0].row_number, null);
});

test("persistence failures are terminalized best-effort and rethrow the original database error", async () => {
  const db = new MemoryImportDb();
  const original = new Error("stage insert failed");
  db.stageInsertError = original;

  await assert.rejects(
    () => runRecordImport(baseInput(db, "import-persist-fail")),
    (error) => error === original,
  );

  assert.equal(db.batches.get("import-persist-fail").status, "FAILED");
  assert.equal(db.stage.length, 0);
  assert.equal(db.issues.length, 1);
  assert.equal(db.issues[0].issue_code, "IMPORT_PERSISTENCE_ERROR");
});

test("recovery failure never replaces the original persistence error", async () => {
  const db = new MemoryImportDb();
  const original = new Error("stage insert failed");
  db.stageInsertError = original;
  db.batchIssueError = new Error("recovery issue insert failed");

  await assert.rejects(
    () => runRecordImport(baseInput(db, "import-recovery-fail")),
    (error) => error === original,
  );

  assert.equal(db.batches.get("import-recovery-fail").status, "RECEIVED");
  assert.equal(db.issues.length, 0);
});

test("duplicate import IDs preserve the existing duplicate exception", async () => {
  const db = new MemoryImportDb();
  db.batches.set("import-duplicate", {
    import_id: "import-duplicate",
    schema_version: "v1",
    status: "RECEIVED",
  });

  await assert.rejects(
    () => runRecordImport(baseInput(db, "import-duplicate")),
    { message: "Import batch already exists: import-duplicate" },
  );
});

test("missing committed summary is an operational invariant failure", async () => {
  const db = new MemoryImportDb();
  db.returnMissingSummary = true;

  await assert.rejects(
    () => runRecordImport(baseInput(db, "import-summary-missing")),
    { message: "Import summary missing after terminalization: import-summary-missing" },
  );
});
