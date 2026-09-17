import {
  withTransaction,
  type PostgresQueryable,
} from "./postgres.js";

export type ImportBatchStatus = "RECEIVED" | "VALIDATING" | "VALIDATED" | "FAILED";

export type ImportSourceKind = "CSV_TEXT" | "LOCAL_FILE";

export type ImportSourceMetadata = {
  sourceKind: ImportSourceKind | null;
  sourceName: string | null;
  sourceSizeBytes: number | null;
  sourceSha256: string | null;
  sourcePath: string | null;
};

export type ImportBatch = ImportSourceMetadata & {
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

export type ImportSummary = ImportSourceMetadata & {
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

type SourceRow = {
  source_kind: ImportSourceKind | null;
  source_name: string | null;
  source_size_bytes: string | number | null;
  source_sha256: string | null;
  source_path: string | null;
};

type BatchRow = SourceRow & {
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

type SummaryRow = SourceRow & {
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

function mapImportSource(row: SourceRow): ImportSourceMetadata {
  return {
    sourceKind: row.source_kind,
    sourceName: row.source_name,
    sourceSizeBytes: row.source_size_bytes === null ? null : Number(row.source_size_bytes),
    sourceSha256: row.source_sha256,
    sourcePath: row.source_path,
  };
}

function mapImportBatch(row: BatchRow): ImportBatch {
  return {
    ...mapImportSource(row),
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
    ...mapImportSource(row),
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
  db: PostgresQueryable,
  input: { importId: string; schemaVersion: string } & Partial<ImportSourceMetadata>,
): Promise<ImportBatch> {
  try {
    const result = await db.query<BatchRow>(
      `insert into import_batch
         (import_id, schema_version, status, source_kind, source_name, source_size_bytes, source_sha256, source_path)
       values ($1, $2, 'RECEIVED', $3, $4, $5, $6, $7)
       returning import_id, schema_version, status, created_at, updated_at,
         source_kind, source_name, source_size_bytes, source_sha256, source_path`,
      [input.importId, input.schemaVersion, input.sourceKind ?? null, input.sourceName ?? null,
        input.sourceSizeBytes ?? null, input.sourceSha256 ?? null, input.sourcePath ?? null],
    );
    return mapImportBatch(result.rows[0]);
  } catch (error) {
    if (isPostgresUniqueViolation(error)) {
      throw new Error(`Import batch already exists: ${input.importId}`);
    }
    throw error;
  }
}

export async function updateImportSourceContentMetadata(
  db: PostgresQueryable,
  input: { importId: string; sourceSizeBytes: number; sourceSha256: string },
): Promise<void> {
  const result = await db.query(
    `update import_batch
     set source_size_bytes = $2, source_sha256 = $3
     where import_id = $1 and status = 'RECEIVED'
     returning import_id`,
    [input.importId, input.sourceSizeBytes, input.sourceSha256],
  );
  if (result.rowCount !== 1) {
    throw new Error("Import must be in RECEIVED status before updating source content metadata.");
  }
}

export async function failImportBatch(
  db: PostgresQueryable,
  input: { importId: string; issueCode: string; detail: string },
): Promise<void> {
  await withTransaction(db, async (transaction) => {
    const transition = await transaction.query(
      `update import_batch
       set status = 'FAILED'
       where import_id = $1 and status = 'RECEIVED'
       returning import_id`,
      [input.importId],
    );
    if (transition.rowCount !== 1) {
      throw new Error("Import must be in RECEIVED status before failure terminalization.");
    }
    await transaction.query(
      `insert into import_issue
         (import_id, row_number, record_id, issue_code, severity, field_key, detail)
       values ($1, null, null, $2, 'ERROR', null, $3)`,
      [input.importId, input.issueCode, input.detail],
    );
  });
}

export async function getImportBatch(db: PostgresQueryable, importId: string): Promise<ImportBatch | null> {
  const result = await db.query<BatchRow>(
    `select import_id, schema_version, status, created_at, updated_at,
       source_kind, source_name, source_size_bytes, source_sha256, source_path
     from import_batch
     where import_id = $1`,
    [importId],
  );
  return result.rows[0] ? mapImportBatch(result.rows[0]) : null;
}

export async function listImportRows(
  db: PostgresQueryable,
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
  db: PostgresQueryable,
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

export async function getImportSummary(db: PostgresQueryable, importId: string): Promise<ImportSummary | null> {
  const result = await db.query<SummaryRow>(
    `select
       b.import_id,
       b.schema_version,
       b.status,
       b.source_kind,
       b.source_name,
       b.source_size_bytes,
       b.source_sha256,
       b.source_path,
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
