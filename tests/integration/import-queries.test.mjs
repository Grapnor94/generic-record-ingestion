import test from "node:test";
import assert from "node:assert/strict";
import {
  createImportBatch,
  failImportBatch,
  getImportBatch,
  listImportRows,
  listImportIssues,
  listImportRowsPage,
  listImportIssuesPage,
  getImportSummary,
} from "../../dist/db/imports.js";
import { encodeIssueCursor, encodeRowCursor } from "../../dist/db/pagination.js";
import { FrameworkError } from "../../dist/errors.js";

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

const nullSourceRow = { source_kind: null, source_name: null, source_size_bytes: null, source_sha256: null, source_path: null };
const nullSource = { sourceKind: null, sourceName: null, sourceSizeBytes: null, sourceSha256: null, sourcePath: null };

const batchRow = {
  ...nullSourceRow,
  import_id: "IMP-1",
  schema_version: "v1",
  status: "RECEIVED",
  created_at: new Date("2026-09-14T04:00:00Z"),
  updated_at: new Date("2026-09-14T04:00:00Z"),
};

test("createImportBatch inserts unchanged identifiers and maps the returned batch", async () => {
  const db = new ScriptedDb([{ result: { rowCount: 1, rows: [batchRow] } }]);
  const batch = await createImportBatch(db, { importId: " IMP-1 ", schemaVersion: " v1 " });
  assert.deepEqual(db.calls[0].values, [" IMP-1 ", " v1 ", null, null, null, null, null]);
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

test("listImportRowsPage fetches one extra row and derives a cursor from the last returned row", async () => {
  const rows = [1, 2, 3].map(rowNumber => ({
    import_id: "IMP-1",
    row_number: String(rowNumber),
    record_id: `R-${rowNumber}`,
    source_row: { rowNumber },
    raw_source_row: null,
    validation_status: rowNumber === 2 ? "INVALID" : "VALID",
  }));
  const db = new ScriptedDb([{ result: { rowCount: 3, rows } }]);

  const page = await listImportRowsPage(db, "IMP-1", { pageSize: 2 });

  assert.deepEqual(page.items.map(row => row.rowNumber), [1, 2]);
  assert.equal(page.nextCursor, encodeRowCursor(2));
  assert.match(db.calls[0].sql, /order by row_number asc\s+limit \$2/i);
  assert.deepEqual(db.calls[0].values, ["IMP-1", 3]);
});

test("listImportRowsPage parameterizes status and cursor filters and returns a terminal page", async () => {
  const db = new ScriptedDb([{ result: { rowCount: 1, rows: [{
    import_id: "IMP-1",
    row_number: "8",
    record_id: "R-8",
    source_row: { rowNumber: 8 },
    raw_source_row: null,
    validation_status: "INVALID",
  }] } }]);
  const cursor = encodeRowCursor(5);

  const page = await listImportRowsPage(db, "IMP-1", {
    pageSize: 2,
    cursor,
    status: "INVALID",
  });

  assert.deepEqual(page.items.map(row => row.rowNumber), [8]);
  assert.equal(page.nextCursor, null);
  assert.match(db.calls[0].sql, /validation_status = \$2/i);
  assert.match(db.calls[0].sql, /row_number > \$3/i);
  assert.match(db.calls[0].sql, /limit \$4/i);
  assert.deepEqual(db.calls[0].values, ["IMP-1", "INVALID", 5, 3]);
  assert.doesNotMatch(db.calls[0].sql, /row_number > 5/);
});

test("listImportIssuesPage preserves null-first keyset ordering across a page boundary", async () => {
  const rows = [
    { issue_id: "10", import_id: "IMP-1", row_number: null, record_id: null, issue_code: "A", severity: "ERROR", field_key: null, detail: "A" },
    { issue_id: "12", import_id: "IMP-1", row_number: null, record_id: null, issue_code: "B", severity: "ERROR", field_key: null, detail: "B" },
    { issue_id: "4", import_id: "IMP-1", row_number: "1", record_id: "R-1", issue_code: "C", severity: "WARNING", field_key: null, detail: "C" },
  ];
  const db = new ScriptedDb([{ result: { rowCount: 3, rows } }]);

  const page = await listImportIssuesPage(db, "IMP-1", { pageSize: 2 });

  assert.deepEqual(page.items.map(issue => [issue.rowNumber, issue.issueId]), [[null, 10], [null, 12]]);
  assert.equal(page.nextCursor, encodeIssueCursor(null, 12));
  assert.match(db.calls[0].sql, /order by row_number asc nulls first, issue_id asc\s+limit \$2/i);
  assert.deepEqual(db.calls[0].values, ["IMP-1", 3]);
});

test("listImportIssuesPage parameterizes filters and both nullable cursor coordinates", async () => {
  const cases = [
    [encodeIssueCursor(null, 12), null],
    [encodeIssueCursor(4, 19), 4],
  ];
  for (const [cursor, cursorRowNumber] of cases) {
    const db = new ScriptedDb([{ result: { rowCount: 0, rows: [] } }]);
    const page = await listImportIssuesPage(db, "IMP-1", {
      pageSize: 5,
      cursor,
      severity: "ERROR",
      rowNumber: 4,
    });

    assert.deepEqual(page, { items: [], nextCursor: null });
    assert.match(db.calls[0].sql, /severity = \$2/i);
    assert.match(db.calls[0].sql, /row_number = \$3/i);
    assert.match(db.calls[0].sql, /\$4::bigint is null/i);
    assert.match(db.calls[0].sql, /issue_id > \$5/i);
    assert.match(db.calls[0].sql, /row_number > \$4/i);
    assert.match(db.calls[0].sql, /row_number = \$4 and issue_id > \$5/i);
    assert.match(db.calls[0].sql, /limit \$6/i);
    assert.deepEqual(db.calls[0].values, ["IMP-1", "ERROR", 4, cursorRowNumber, cursorRowNumber === null ? 12 : 19, 6]);
    assert.doesNotMatch(db.calls[0].sql, /issue_id > (12|19)/);
  }
});

test("paginated import queries reject invalid cursors and page sizes before querying", async () => {
  for (const call of [
    db => listImportRowsPage(db, "IMP-1", { pageSize: 0 }),
    db => listImportRowsPage(db, "IMP-1", { cursor: encodeIssueCursor(null, 1) }),
    db => listImportIssuesPage(db, "IMP-1", { pageSize: 1001 }),
    db => listImportIssuesPage(db, "IMP-1", { cursor: encodeRowCursor(1) }),
  ]) {
    const db = new ScriptedDb();
    await assert.rejects(
      () => call(db),
      error => error instanceof FrameworkError
        && (error.code === "INVALID_PAGE_SIZE" || error.code === "INVALID_CURSOR"),
    );
    assert.equal(db.calls.length, 0);
  }
});

test("getImportSummary maps independent aggregate counts including zeroes", async () => {
  const db = new ScriptedDb([{ result: { rowCount: 1, rows: [{
    import_id: "IMP-1",
    schema_version: "v1",
    status: "FAILED",
    ...nullSourceRow,
    row_count: "3",
    valid_row_count: "1",
    invalid_row_count: "1",
    pending_row_count: "1",
    error_count: "2",
    warning_count: "4",
  }] } }]);
  const summary = await getImportSummary(db, "IMP-1");
  assert.deepEqual(summary, {
    ...nullSource,
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

test("failImportBatch terminalizes RECEIVED batches and inserts one batch-level issue", async () => {
  const db = new ScriptedDb([
    { result: { rowCount: null, rows: [] } },
    { result: { rowCount: 1, rows: [{ import_id: "IMP-1" }] } },
    { result: { rowCount: 1, rows: [] } },
    { result: { rowCount: null, rows: [] } },
  ]);

  await failImportBatch(db, {
    importId: "IMP-1",
    issueCode: "CSV_PARSE_ERROR",
    detail: "bad csv",
  });

  assert.equal(db.calls.length, 4);
  assert.match(db.calls[0].sql, /^begin$/i);
  assert.match(db.calls[1].sql, /update import_batch/i);
  assert.match(db.calls[1].sql, /status = 'RECEIVED'/i);
  assert.deepEqual(db.calls[1].values, ["IMP-1"]);
  assert.match(db.calls[2].sql, /insert into import_issue/i);
  assert.deepEqual(db.calls[2].values, ["IMP-1", "CSV_PARSE_ERROR", "bad csv"]);
  assert.match(db.calls[3].sql, /^commit$/i);
});

test("failImportBatch rejects non-RECEIVED batches and rolls back without inserting an issue", async () => {
  const db = new ScriptedDb([
    { result: { rowCount: null, rows: [] } },
    { result: { rowCount: 0, rows: [] } },
    { result: { rowCount: null, rows: [] } },
  ]);

  await assert.rejects(
    () => failImportBatch(db, { importId: "IMP-1", issueCode: "CSV_PARSE_ERROR", detail: "bad csv" }),
    { message: "Import must be in RECEIVED status before failure terminalization." },
  );

  assert.equal(db.calls.length, 3);
  assert.match(db.calls[2].sql, /^rollback$/i);
  assert.equal(db.calls.some((call) => /insert into import_issue/i.test(call.sql)), false);
});

test("failImportBatch rolls back when batch-level issue persistence fails", async () => {
  const insertFailure = new Error("issue insert failed");
  const db = new ScriptedDb([
    { result: { rowCount: null, rows: [] } },
    { result: { rowCount: 1, rows: [{ import_id: "IMP-1" }] } },
    { error: insertFailure },
    { result: { rowCount: null, rows: [] } },
  ]);

  await assert.rejects(
    () => failImportBatch(db, { importId: "IMP-1", issueCode: "CSV_PARSE_ERROR", detail: "bad csv" }),
    (error) => error === insertFailure,
  );

  assert.match(db.calls.at(-1).sql, /^rollback$/i);
});
