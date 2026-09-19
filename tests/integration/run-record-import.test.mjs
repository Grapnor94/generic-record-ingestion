import test from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
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
    this.batchInsertCount = 0;
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
      this.batchInsertCount += 1;
      const [importId, schemaVersion, source_kind, source_name, source_size_bytes, source_sha256, source_path] = values;
      if (this.batches.has(importId)) {
        throw Object.assign(new Error("duplicate"), { code: "23505" });
      }
      const row = {
        import_id: importId,
        schema_version: schemaVersion,
        source_kind, source_name, source_size_bytes, source_sha256, source_path,
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
          ...batch,
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

function assertTextSource(batch, csvText) {
  assert.equal(batch.source_kind, "CSV_TEXT");
  assert.equal(batch.source_name, null);
  assert.equal(batch.source_path, null);
  assert.equal(batch.source_size_bytes, Buffer.byteLength(csvText, "utf8"));
  assert.equal(batch.source_sha256, createHash("sha256").update(csvText, "utf8").digest("hex"));
}

test("CSV provenance is durable while RECEIVED before callbacks and counts multibyte bytes", async () => {
  const db = new MemoryImportDb();
  const csvText = "id,name\n1,é😀\n";
  let called = false;
  const result = await runRecordImport({ ...baseInput(db, "unicode"), csvText, transform(row) {
    called = true;
    const batch = db.batches.get("unicode");
    assert.equal(batch.status, "RECEIVED");
    assertTextSource(batch, csvText);
    return transform(row);
  } });
  assert.equal(called, true);
  assert.equal(result.summary.sourceKind, "CSV_TEXT");
  assert.equal(result.summary.sourceSizeBytes, 17);
  assert.equal(result.summary.sourceSha256, db.batches.get("unicode").source_sha256);
  assert.equal(result.summary.sourceName, null);
  assert.equal(result.summary.sourcePath, null);
});

test("invalid CSV ingestion limits reject before creating a batch", async () => {
  const db = new MemoryImportDb();

  await assert.rejects(
    () => runRecordImport({
      ...baseInput(db, "invalid-limit"),
      limits: { maxSourceBytes: 0 },
    }),
    { code: "INVALID_INGESTION_LIMIT" },
  );

  assert.equal(db.batchInsertCount, 0);
  assert.equal(db.batches.size, 0);
});

test("CSV source exactly at its UTF-8 byte limit succeeds", async () => {
  const db = new MemoryImportDb();
  const csvText = "id,name\n1,é😀\n";
  const result = await runRecordImport({
    ...baseInput(db, "source-limit-exact"),
    csvText,
    limits: { maxSourceBytes: 17, maxDataRows: 1 },
  });

  assert.equal(result.status, "VALIDATED");
  assert.equal(result.summary.rowCount, 1);
  assertTextSource(db.batches.get("source-limit-exact"), csvText);
});

test("CSV source one byte over its UTF-8 limit fails durably before callbacks", async () => {
  const db = new MemoryImportDb();
  const csvText = "id,name\n1,é😀\n";
  let callbackCalls = 0;
  const result = await runRecordImport({
    ...baseInput(db, "source-limit-over"),
    csvText,
    limits: { maxSourceBytes: 16, maxDataRows: 1 },
    transform: () => { callbackCalls += 1; return {}; },
    getRecordId: () => { callbackCalls += 1; return null; },
    diagnose: () => { callbackCalls += 1; return []; },
  });

  assert.equal(result.status, "FAILED");
  assert.equal(result.summary.status, "FAILED");
  assert.equal(result.summary.rowCount, 0);
  assertTextSource(db.batches.get("source-limit-over"), csvText);
  assert.equal(db.issues[0].issue_code, "SOURCE_SIZE_LIMIT_EXCEEDED");
  assert.equal(db.stage.length, 0);
  assert.equal(callbackCalls, 0);
});

test("CSV data row limit is inclusive and overflow never stages a prefix", async () => {
  const db = new MemoryImportDb();
  const exact = await runRecordImport({
    ...baseInput(db, "row-limit-exact"),
    limits: { maxDataRows: 1 },
  });

  let callbackCalls = 0;
  const overflowCsv = "id,name\n1,Alice\n2\n";
  const overflow = await runRecordImport({
    ...baseInput(db, "row-limit-over"),
    csvText: overflowCsv,
    limits: { maxDataRows: 1 },
    transform: () => { callbackCalls += 1; return {}; },
    getRecordId: () => { callbackCalls += 1; return null; },
    diagnose: () => { callbackCalls += 1; return []; },
  });

  assert.equal(exact.status, "VALIDATED");
  assert.equal(exact.summary.rowCount, 1);
  assert.equal(overflow.status, "FAILED");
  assert.equal(overflow.summary.rowCount, 0);
  assertTextSource(db.batches.get("row-limit-over"), overflowCsv);
  assert.equal(db.issues.find((issue) => issue.import_id === "row-limit-over").issue_code, "ROW_LIMIT_EXCEEDED");
  assert.equal(db.stage.filter((row) => row.import_id === "row-limit-over").length, 0);
  assert.equal(callbackCalls, 0);
});

for (const [label, csvText, diagnose, code] of [
  ["parse", 'id,name\n1,"bad\n', undefined, "CSV_PARSE_ERROR"],
  ["header", "id,other\n1,Alice\n", undefined, "SCHEMA_HEADER_ERROR"],
  ["row", "id,name\n1,Alice\n", () => [{ code: "BAD", severity: "ERROR", fieldKey: null, detail: "bad" }], "BAD"],
]) {
  test(`CSV ${label} failures retain complete provenance`, async () => {
    const db = new MemoryImportDb();
    const result = await runRecordImport({ ...baseInput(db, label), csvText, diagnose });
    assert.equal(result.status, "FAILED");
    assertTextSource(db.batches.get(label), csvText);
    assert.equal(db.issues[0].issue_code, code);
    assert.equal(result.summary.sourceSha256, db.batches.get(label).source_sha256);
  });
}

for (const kind of ["callback", "persistence", "recovery"]) {
  test(`CSV ${kind} exception retains provenance and original error identity`, async () => {
    const db = new MemoryImportDb();
    const original = new Error(kind);
    const input = baseInput(db, kind);
    if (kind === "callback") input.transform = () => { throw original; };
    else db.stageInsertError = original;
    if (kind === "recovery") db.batchIssueError = new Error("recovery failed");
    await assert.rejects(() => runRecordImport(input), e => e === original);
    assertTextSource(db.batches.get(kind), input.csvText);
    assert.equal(db.batches.get(kind).status, kind === "recovery" ? "RECEIVED" : "FAILED");
  });
}

test("identical CSV content remains accepted under different import IDs", async () => {
  const db = new MemoryImportDb();
  for (const id of ["same-a", "same-b"]) {
    const result = await runRecordImport(baseInput(db, id));
    assert.equal(result.status, "VALIDATED");
    assertTextSource(db.batches.get(id), baseInput(db, id).csvText);
  }
  assert.equal(db.batches.get("same-a").source_sha256, db.batches.get("same-b").source_sha256);
  assert.equal(db.stage.length, 2);
});

function baseInput(db, importId) {
  return {
    db: { kind: "CLIENT", client: db },
    importId,
    contract,
    csvText: "id,name\n1,Alice\n",
    transform,
    getRecordId,
  };
}

test("pool workflow acquires and releases one dedicated connection", async () => {
  const client = new MemoryImportDb();
  client.releaseCalls = 0;
  client.release = function release() { this.releaseCalls += 1; };
  const pool = {
    connectCalls: 0,
    async connect() {
      this.connectCalls += 1;
      return client;
    },
    async query() {
      throw new Error("workflow must not use pool.query");
    },
  };

  const result = await runRecordImport({
    ...baseInput(client, "pool-import"),
    db: { kind: "POOL", pool },
  });

  assert.equal(result.status, "VALIDATED");
  assert.equal(pool.connectCalls, 1);
  assert.equal(client.releaseCalls, 1);
});

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
      sourceKind: "CSV_TEXT", sourceName: null, sourceSizeBytes: 22,
      sourceSha256: "2e9f445b06e5ca0fb9638798078f6a239ea1fcace10e1dcfb70ceb3a3322ad44", sourcePath: null,
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

test("text imports still create exactly one durable batch", async () => {
  const db = new MemoryImportDb();
  const result = await runRecordImport(baseInput(db, "text-regression"));

  assert.equal(result.status, "VALIDATED");
  assert.equal(db.batchInsertCount, 1);
  assert.equal(result.summary.rowCount, 1);
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

test("duplicate CSV headers become a durable batch-level DUPLICATE_HEADER before callbacks", async () => {
  const db = new MemoryImportDb();
  const csvText = "id,name,name\n1,Alice,Alias\n2,Bob,Robert\n";
  let transformed = false;
  let recordIdRead = false;
  let diagnosed = false;

  const result = await runRecordImport({
    ...baseInput(db, "import-duplicate-header"),
    csvText,
    limits: { maxDataRows: 1 },
    transform: () => { transformed = true; return {}; },
    getRecordId: () => { recordIdRead = true; return null; },
    diagnose: () => { diagnosed = true; return []; },
  });

  assert.equal(result.status, "FAILED");
  assert.equal(result.summary.status, "FAILED");
  assert.equal(result.summary.rowCount, 0);
  assert.equal(result.summary.errorCount, 1);
  assertTextSource(db.batches.get("import-duplicate-header"), csvText);
  assert.equal(db.stage.length, 0);
  assert.equal(db.issues.length, 1);
  assert.equal(db.issues[0].issue_code, "DUPLICATE_HEADER");
  assert.equal(db.issues[0].row_number, null);
  assert.equal(db.issues[0].record_id, null);
  assert.equal(transformed, false);
  assert.equal(recordIdRead, false);
  assert.equal(diagnosed, false);
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
