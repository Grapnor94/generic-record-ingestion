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

    if (q.startsWith("update import_batch") && q.includes("validating")) {
      const [importId] = values;
      const batch = this.batches.get(importId);
      if (!batch || batch.status !== "RECEIVED") return { rowCount: 0, rows: [] };
      batch.status = "VALIDATING";
      return { rowCount: 1, rows: [{ import_id: importId }] };
    }

    if (q.startsWith("insert into import_stage_row")) {
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

test("runRecordImport persists a valid CSV import and returns the committed summary", async () => {
  const db = new MemoryImportDb();

  const result = await runRecordImport({
    db,
    importId: "import-ok",
    contract,
    csvText: "id,name\n1,Alice\n2,Bob\n",
    transform,
    getRecordId,
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
