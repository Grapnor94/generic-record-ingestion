import type { Queryable } from "../ingestion/persist-record-staging.js";

export type ImportBatchStatus = "RECEIVED" | "VALIDATING" | "VALIDATED" | "FAILED";

export type ImportBatch = {
  importId: string;
  schemaVersion: string;
  status: ImportBatchStatus;
  createdAt: Date;
  updatedAt: Date;
};

export type ImportRowStatus = "PENDING" | "VALID" | "INVALID";

export type ImportRow = {
  importId: string;
  rowNumber: number;
  recordId: string | null;
  sourceRow: Record<string, unknown>;
  rawSourceRow: Record<string, unknown> | null;
  validationStatus: ImportRowStatus;
};

export type ImportIssueSeverity = "ERROR" | "WARNING";

export type ImportIssue = {
  issueId: number;
  importId: string;
  rowNumber: number | null;
  recordId: string | null;
  issueCode: string;
  severity: ImportIssueSeverity;
  fieldKey: string | null;
  detail: string;
};

export type ImportSummary = {
  importId: string;
  schemaVersion: string;
  status: ImportBatchStatus;
  rowCount: number;
  validRowCount: number;
  invalidRowCount: number;
  pendingRowCount: number;
  errorCount: number;
  warningCount: number;
};

type BatchRow = {
  import_id: string;
  schema_version: string;
  status: ImportBatchStatus;
  created_at: Date;
  updated_at: Date;
};

type StageRow = {
  import_id: string;
  row_number: string | number;
  record_id: string | null;
  source_row: Record<string, unknown>;
  raw_source_row: Record<string, unknown> | null;
  validation_status: ImportRowStatus;
};

type IssueRow = {
  issue_id: string | number;
  import_id: string;
  row_number: string | number | null;
  record_id: string | null;
  issue_code: string;
  severity: ImportIssueSeverity;
  field_key: string | null;
  detail: string;
};

type SummaryRow = {
  import_id: string;
  schema_version: string;
  status: ImportBatchStatus;
  row_count: string | number;
  valid_row_count: string | number;
  invalid_row_count: string | number;
  pending_row_count: string | number;
  error_count: string | number;
  warning_count: string | number;
};

function mapImportBatch(row: BatchRow): ImportBatch {
  return {
    importId: row.import_id,
    schemaVersion: row.schema_version,
    status: row.status,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function mapImportRow(row: StageRow): ImportRow {
  return {
    importId: row.import_id,
    rowNumber: Number(row.row_number),
    recordId: row.record_id,
    sourceRow: row.source_row,
    rawSourceRow: row.raw_source_row,
    validationStatus: row.validation_status,
  };
}

function mapImportIssue(row: IssueRow): ImportIssue {
  return {
    issueId: Number(row.issue_id),
    importId: row.import_id,
    rowNumber: row.row_number === null ? null : Number(row.row_number),
    recordId: row.record_id,
    issueCode: row.issue_code,
    severity: row.severity,
    fieldKey: row.field_key,
    detail: row.detail,
  };
}

function mapImportSummary(row: SummaryRow): ImportSummary {
  return {
    importId: row.import_id,
    schemaVersion: row.schema_version,
    status: row.status,
    rowCount: Number(row.row_count),
    validRowCount: Number(row.valid_row_count),
    invalidRowCount: Number(row.invalid_row_count),
    pendingRowCount: Number(row.pending_row_count),
    errorCount: Number(row.error_count),
    warningCount: Number(row.warning_count),
  };
}

function isPostgresUniqueViolation(error: unknown): boolean {
  return typeof error === "object" && error !== null && "code" in error && error.code === "23505";
}

export async function createImportBatch(
  db: Queryable,
  input: { importId: string; schemaVersion: string },
): Promise<ImportBatch> {
  try {
    const result = await db.query<BatchRow>(
      `insert into import_batch (import_id, schema_version, status)
       values ($1, $2, 'RECEIVED')
       returning import_id, schema_version, status, created_at, updated_at`,
      [input.importId, input.schemaVersion],
    );
    return mapImportBatch(result.rows[0]);
  } catch (error) {
    if (isPostgresUniqueViolation(error)) {
      throw new Error(`Import batch already exists: ${input.importId}`);
    }
    throw error;
  }
}

export async function getImportBatch(db: Queryable, importId: string): Promise<ImportBatch | null> {
  const result = await db.query<BatchRow>(
    `select import_id, schema_version, status, created_at, updated_at
     from import_batch
     where import_id = $1`,
    [importId],
  );
  return result.rows[0] ? mapImportBatch(result.rows[0]) : null;
}

export async function listImportRows(
  db: Queryable,
  importId: string,
  options?: { status?: ImportRowStatus },
): Promise<ImportRow[]> {
  const values: unknown[] = [importId];
  let filter = "";
  if (options?.status) {
    values.push(options.status);
    filter = " and validation_status = $2";
  }
  const result = await db.query<StageRow>(
    `select import_id, row_number, record_id, source_row, raw_source_row, validation_status
     from import_stage_row
     where import_id = $1${filter}
     order by row_number asc`,
    values,
  );
  return result.rows.map(mapImportRow);
}

export async function listImportIssues(
  db: Queryable,
  importId: string,
  options?: { severity?: ImportIssueSeverity; rowNumber?: number },
): Promise<ImportIssue[]> {
  const values: unknown[] = [importId];
  const filters: string[] = [];
  if (options?.severity) {
    values.push(options.severity);
    filters.push(`severity = $${values.length}`);
  }
  if (options?.rowNumber !== undefined) {
    values.push(options.rowNumber);
    filters.push(`row_number = $${values.length}`);
  }
  const suffix = filters.length > 0 ? ` and ${filters.join(" and ")}` : "";
  const result = await db.query<IssueRow>(
    `select issue_id, import_id, row_number, record_id, issue_code, severity, field_key, detail
     from import_issue
     where import_id = $1${suffix}
     order by row_number asc nulls first, issue_id asc`,
    values,
  );
  return result.rows.map(mapImportIssue);
}

export async function getImportSummary(db: Queryable, importId: string): Promise<ImportSummary | null> {
  const result = await db.query<SummaryRow>(
    `select
       b.import_id,
       b.schema_version,
       b.status,
       coalesce(r.row_count, 0) as row_count,
       coalesce(r.valid_row_count, 0) as valid_row_count,
       coalesce(r.invalid_row_count, 0) as invalid_row_count,
       coalesce(r.pending_row_count, 0) as pending_row_count,
       coalesce(i.error_count, 0) as error_count,
       coalesce(i.warning_count, 0) as warning_count
     from import_batch b
     left join lateral (
       select
         count(*) as row_count,
         count(*) filter (where validation_status = 'VALID') as valid_row_count,
         count(*) filter (where validation_status = 'INVALID') as invalid_row_count,
         count(*) filter (where validation_status = 'PENDING') as pending_row_count
       from import_stage_row
       where import_id = b.import_id
     ) r on true
     left join lateral (
       select
         count(*) filter (where severity = 'ERROR') as error_count,
         count(*) filter (where severity = 'WARNING') as warning_count
       from import_issue
       where import_id = b.import_id
     ) i on true
     where b.import_id = $1`,
    [importId],
  );
  return result.rows[0] ? mapImportSummary(result.rows[0]) : null;
}
