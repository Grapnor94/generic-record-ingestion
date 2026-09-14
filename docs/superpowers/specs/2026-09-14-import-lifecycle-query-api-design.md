# Import Lifecycle and Query API Design

**Date:** 2026-09-14
**Repository:** `Grapnor94/generic-record-ingestion`
**Status:** Approved design, pending implementation

## Goal

Add a small, domain-neutral database service layer around the existing generic record-ingestion tables so callers can create import batches and inspect batch state, staged rows, validation issues, and aggregate import results without duplicating lifecycle logic already owned by persistence.

## Scope

This phase adds database-facing query and lifecycle functions only. It does not add CSV parsing, downstream publication, deletion/retry workflows, arbitrary status mutation, application-specific schemas, or domain-specific record semantics.

The API must remain framework-neutral and accept the existing `Queryable` interface used by `persistRecordStaging`.

## Existing Lifecycle Contract

`persistRecordStaging` already owns the validation lifecycle transition:

`RECEIVED -> VALIDATING -> VALIDATED | FAILED`

This phase must preserve that ownership. It must not expose a generic status setter that lets callers force arbitrary lifecycle states.

## Public API

Create `src/db/imports.ts` with the following exported types and functions.

### Types

```ts
export type ImportBatchStatus =
  | "RECEIVED"
  | "VALIDATING"
  | "VALIDATED"
  | "FAILED";

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
```

### `createImportBatch`

```ts
export async function createImportBatch(
  db: Queryable,
  input: { importId: string; schemaVersion: string },
): Promise<ImportBatch>;
```

Behavior:
- Inserts exactly one `import_batch` row in `RECEIVED` status.
- Returns the inserted batch.
- Does not trim or mutate the supplied identifiers.
- Relies on the database primary key to reject duplicate `importId` values.
- Converts a PostgreSQL unique-violation (`23505`) into a stable package error: `Import batch already exists: <importId>`.
- Other database errors propagate unchanged.

### `getImportBatch`

```ts
export async function getImportBatch(
  db: Queryable,
  importId: string,
): Promise<ImportBatch | null>;
```

Behavior:
- Returns the matching batch or `null`.
- Does not create or mutate data.

### `listImportRows`

```ts
export async function listImportRows(
  db: Queryable,
  importId: string,
  options?: { status?: ImportRowStatus },
): Promise<ImportRow[]>;
```

Behavior:
- Returns rows for one import ordered by `row_number ASC`.
- Optional `status` filter accepts only the compile-time union `PENDING | VALID | INVALID`.
- Returns an empty array when the import has no rows or does not exist.
- No pagination in this version.

### `listImportIssues`

```ts
export async function listImportIssues(
  db: Queryable,
  importId: string,
  options?: {
    severity?: ImportIssueSeverity;
    rowNumber?: number;
  },
): Promise<ImportIssue[]>;
```

Behavior:
- Filters by import ID and optionally by severity and/or row number.
- Results are deterministic: order by `row_number ASC NULLS FIRST, issue_id ASC`.
- Returns an empty array when no issues match.
- No pagination in this version.

### `getImportSummary`

```ts
export async function getImportSummary(
  db: Queryable,
  importId: string,
): Promise<ImportSummary | null>;
```

Behavior:
- Returns `null` when the import batch does not exist.
- Returns one aggregate object when it exists, including zero counts when no rows/issues exist.
- Counts row states from `import_stage_row.validation_status`.
- Counts diagnostic severities from `import_issue.severity`.
- Must avoid a row/issues join that multiplies counts. Use independent aggregate subqueries or equivalent aggregation.

## SQL and Mapping Rules

Database columns remain snake_case. Public TypeScript objects use camelCase.

JSONB values returned by PostgreSQL are exposed as `Record<string, unknown>` without additional transformation.

`row_number` and `issue_id` are represented as JavaScript `number` in this version. Tests must keep values within JavaScript's safe integer range. Supporting values larger than `Number.MAX_SAFE_INTEGER` is out of scope for V0.3.

Timestamps are returned as `Date` values when using the PostgreSQL driver. In-memory tests may use `Date` instances directly.

## Error Handling

The package defines only one new stable domain-neutral error translation in this phase: duplicate import creation.

All other SQL/connection failures propagate to callers unchanged. Query functions do not hide database failures by returning empty results or `null`.

Missing data is not an error:
- `getImportBatch` -> `null`
- `getImportSummary` -> `null`
- row/issue list functions -> `[]`

## Database Changes

No new migration is required for the initial implementation. Existing primary keys, foreign keys, and validation columns are sufficient for correctness at the current package scale.

Do not add indexes speculatively. Indexing can be introduced later from measured query behavior.

## Module Boundaries

`src/db/imports.ts` owns import creation and read/query operations.

`src/ingestion/persist-record-staging.ts` continues to own transactional validation persistence and lifecycle transitions.

`src/db/migrations.ts` remains responsible only for schema migration discovery/execution.

The new module may import the existing `Queryable` type from `persist-record-staging.ts`; no new database abstraction is introduced in this phase.

## Testing Strategy

### In-memory database-call tests

Add `tests/integration/import-queries.test.mjs` using a focused fake `Queryable` to verify:
- create success and returned mapping;
- duplicate-ID error translation;
- non-duplicate database failures propagate;
- batch retrieval and missing batch behavior;
- deterministic row ordering query and optional status filter;
- deterministic issue ordering and severity/row filters;
- summary mapping and zero counts;
- missing summary behavior.

The tests should assert SQL intent and parameter ordering rather than emulate PostgreSQL fully.

### Live PostgreSQL tests

Extend `tests/postgres/live-postgres.test.mjs` to verify against an empty schema after migrations:
- `createImportBatch` creates a `RECEIVED` batch;
- duplicate creation fails with the stable package error;
- `getImportBatch` returns persisted metadata;
- after `persistRecordStaging`, row and issue query functions return the expected deterministic data;
- summary counts match persisted VALID/INVALID rows and ERROR/WARNING issues;
- a missing import returns `null`/empty results according to contract.

### Regression verification

Run:
- `npm run typecheck`
- `npm test`
- `npm run test:integration`
- `npm run verify`
- GitHub Actions PostgreSQL integration gate

The existing migration and persistence tests must remain green.

## Documentation

Update `README.md` with a concise "Import lifecycle/query API" section showing the public functions and emphasizing that status transitions remain owned by staging persistence.

Update `VERIFICATION.txt` only with results actually observed. GitHub Actions live PostgreSQL results must not be recorded as PASS until the corresponding workflow run succeeds.

## Explicit Non-Goals

This phase does not include:
- CSV/file parsing;
- streaming imports;
- pagination/cursors;
- retry/reset/delete import operations;
- arbitrary status mutation;
- downstream publication or application tables;
- domain-specific field mappings;
- batch search across many imports;
- new indexes absent measured need.

## Acceptance Criteria

The phase is complete when:
1. all five public functions exist with the signatures and behavior above;
2. lifecycle ownership remains unchanged and no arbitrary status setter exists;
3. unit/in-memory integration verification is green;
4. live PostgreSQL tests cover creation, duplicate detection, reads, filters, summaries, and compatibility with `persistRecordStaging`;
5. the authoritative GitHub Actions PostgreSQL gate passes on the implementation branch and again for the pull request before merge.
