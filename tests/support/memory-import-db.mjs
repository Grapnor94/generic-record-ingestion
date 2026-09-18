export class MemoryImportDb {
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
      const [importId, schemaVersion, source_kind, source_name, source_size_bytes, source_sha256, source_path] = values;
      if (this.batches.has(importId)) throw Object.assign(new Error("duplicate"), { code: "23505" });
      const row = { source_kind, source_name, source_size_bytes, source_sha256, source_path, import_id: importId, schema_version: schemaVersion, status: "RECEIVED", created_at: new Date(), updated_at: new Date() };
      this.batches.set(importId, { ...row });
      return { rowCount: 1, rows: [row] };
    }
    if (q.startsWith("update import_batch") && q.includes("set source_kind = coalesce")) {
      const batch = this.batches.get(values[0]);
      if (!batch || batch.status !== "RECEIVED") return { rowCount: 0, rows: [] };
      const fields = ["schema_version", "source_kind", "source_name", "source_size_bytes", "source_sha256", "source_path"];
      if (fields.some((field, i) => batch[field] !== null && batch[field] !== values[i + 1])) return { rowCount: 0, rows: [] };
      fields.slice(1, 5).forEach((field, i) => { batch[field] ??= values[i + 2]; });
      return { rowCount: 1, rows: [{ import_id: values[0] }] };
    }
    if (q.startsWith("update import_batch") && q.includes("set source_size_bytes")) {
      const batch = this.batches.get(values[0]);
      if (!batch || batch.status !== "RECEIVED") return { rowCount: 0, rows: [] };
      batch.source_size_bytes = values[1];
      batch.source_sha256 = values[2];
      return { rowCount: 1, rows: [{ import_id: values[0] }] };
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
      return { rowCount: 1, rows: [{ ...batch, import_id: importId, schema_version: batch.schema_version, status: batch.status, row_count: rows.length, valid_row_count: rows.filter((r) => r.validation_status === "VALID").length, invalid_row_count: rows.filter((r) => r.validation_status === "INVALID").length, pending_row_count: rows.filter((r) => r.validation_status === "PENDING").length, error_count: issues.filter((i) => i.severity === "ERROR").length, warning_count: issues.filter((i) => i.severity === "WARNING").length }] };
    }
    if (q.startsWith("select") && q.includes("from import_batch")) { const row = this.batches.get(values[0]); return { rowCount: row ? 1 : 0, rows: row ? [{ ...row }] : [] }; }
    throw new Error(`Unsupported SQL: ${q}`);
  }
}
