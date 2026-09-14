import test from "node:test";
import assert from "node:assert/strict";
import {
  createImportBatch,
  getImportBatch,
  listImportRows,
  listImportIssues,
  getImportSummary,
} from "../../dist/db/imports.js";

class ScriptedDb {
  constructor(steps = []) {
    this.steps = [...steps];
    this.calls = [];
  }
  async query(sql, values = []) {
    this.calls.push({ sql, values });
    const step = this.steps.shift();
    if (!step) throw new Error(`Unexpected query: ${sql}`);
    if (step.error) throw step.error;
    return step.result;
  }
}

const batchRow = {
  import_id: "IMP-1",
  schema_version: "v1",
  status: "RECEIVED",
  created_at: new Date("2026-09-14T04:00:00Z"),
  updated_at: new Date("2026-09-14T04:00:00Z"),
};

test("createImportBatch inserts unchanged identifiers and maps the returned batch", async () => {
  const db = new ScriptedDb([{ result: { rowCount: 1, rows: [batchRow] } }]);
  const batch = await createImportBatch(db, { importId: " IMP-1 ", schemaVersion: " v1 " });
  assert.deepEqual(db.calls[0].values, [" IMP-1 ", " v1 "]);
  assert.match(db.calls[0].sql, /insert into import_batch/i);
  assert.equal(batch.importId, "IMP-1");
  assert.equal(batch.schemaVersion, "v1");
  assert.equal(batch.status, "RECEIVED");
  assert.equal(batch.createdAt, batchRow.created_at);
});

test("createImportBatch translates only PostgreSQL duplicate-key errors", async () => {
  const duplicate = Object.assign(new Error("duplicate"), { code: "23505" });
  const db = new ScriptedDb([{ error: duplicate }]);
  await assert.rejects(
    () => createImportBatch(db, { importId: "IMP-1", schemaVersion: "v1" }),
    { message: "Import batch already exists: IMP-1" },
  );
});

test("createImportBatch rethrows non-duplicate database failures unchanged", async () => {
  const failure = Object.assign(new Error("connection lost"), { code: "08006" });
  const db = new ScriptedDb([{ error: failure }]);
  await assert.rejects(
    () => createImportBatch(db, { importId: "IMP-1", schemaVersion: "v1" }),
    (error) => error === failure,
  );
});

test("getImportBatch maps a matching row and returns null when missing", async () => {
  const found = new ScriptedDb([{ result: { rowCount: 1, rows: [batchRow] } }]);
  assert.equal((await getImportBatch(found, "IMP-1"))?.importId, "IMP-1");
  assert.deepEqual(found.calls[0].values, ["IMP-1"]);

  const missing = new ScriptedDb([{ result: { rowCount: 0, rows: [] } }]);
  assert.equal(await getImportBatch(missing, "MISSING"), null);
});

test("listImportRows orders rows and maps bigint identifiers to numbers", async () => {
  const db = new ScriptedDb([{ result: { rowCount: 2, rows: [
    { import_id: "IMP-1", row_number: "1", record_id: "R-1", source_row: { name: "Ada" }, raw_source_row: { name: " Ada " }, validation_status: "VALID" },
    { import_id: "IMP-1", row_number: "2", record_id: null, source_row: { name: "Bob" }, raw_source_row: null, validation_status: "PENDING" },
  ] } }]);
  const rows = await listImportRows(db, "IMP-1");
  assert.match(db.calls[0].sql, /order by row_number asc/i);
  assert.deepEqual(db.calls[0].values, ["IMP-1"]);
  assert.deepEqual(rows.map((row) => row.rowNumber), [1, 2]);
  assert.equal(rows[1].rawSourceRow, null);
});

test("listImportRows adds the optional status filter with stable parameters", async () => {
  const db = new ScriptedDb([{ result: { rowCount: 0, rows: [] } }]);
  assert.deepEqual(await listImportRows(db, "IMP-1", { status: "INVALID" }), []);
  assert.match(db.calls[0].sql, /validation_status = \$2/i);
  assert.deepEqual(db.calls[0].values, ["IMP-1", "INVALID"]);
});

test("listImportIssues orders deterministically and supports all filters", async () => {
  const issueRows = [{ issue_id: "7", import_id: "IMP-1", row_number: "2", record_id: "R-2", issue_code: "BAD", severity: "ERROR", field_key: "name", detail: "Bad value" }];
  for (const [options, values, patterns] of [
    [undefined, ["IMP-1"], []],
    [{ severity: "WARNING" }, ["IMP-1", "WARNING"], [/severity = \$2/i]],
    [{ rowNumber: 3 }, ["IMP-1", 3], [/row_number = \$2/i]],
    [{ severity: "ERROR", rowNumber: 3 }, ["IMP-1", "ERROR", 3], [/severity = \$2/i, /row_number = \$3/i]],
  ]) {
    const db = new ScriptedDb([{ result: { rowCount: 1, rows: issueRows } }]);
    const issues = await listImportIssues(db, "IMP-1", options);
    assert.match(db.calls[0].sql, /order by row_number asc nulls first, issue_id asc/i);
    for (const pattern of patterns) assert.match(db.calls[0].sql, pattern);
    assert.deepEqual(db.calls[0].values, values);
    assert.equal(issues[0].issueId, 7);
    assert.equal(issues[0].rowNumber, 2);
  }
});

test("getImportSummary maps independent aggregate counts including zeroes", async () => {
  const db = new ScriptedDb([{ result: { rowCount: 1, rows: [{
    import_id: "IMP-1",
    schema_version: "v1",
    status: "FAILED",
    row_count: "3",
    valid_row_count: "1",
    invalid_row_count: "1",
    pending_row_count: "1",
    error_count: "2",
    warning_count: "4",
  }] } }]);
  const summary = await getImportSummary(db, "IMP-1");
  assert.deepEqual(summary, {
    importId: "IMP-1", schemaVersion: "v1", status: "FAILED",
    rowCount: 3, validRowCount: 1, invalidRowCount: 1, pendingRowCount: 1,
    errorCount: 2, warningCount: 4,
  });
  assert.match(db.calls[0].sql, /from import_batch/i);
  assert.match(db.calls[0].sql, /import_stage_row/i);
  assert.match(db.calls[0].sql, /import_issue/i);
  assert.doesNotMatch(db.calls[0].sql, /import_stage_row\s+[^;]*join\s+import_issue/is);
});

test("getImportSummary returns null when the batch does not exist", async () => {
  const db = new ScriptedDb([{ result: { rowCount: 0, rows: [] } }]);
  assert.equal(await getImportSummary(db, "MISSING"), null);
});
