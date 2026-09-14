# V0.5 End-to-End Import Orchestration Design

Date: 2026-09-14
Status: Approved design for implementation planning

## Purpose

V0.5 adds one supported application-level operation for running a complete record import from CSV text through persistence while preserving the existing separation between CSV parsing, staging/validation, persistence, and query APIs.

The new orchestration layer must remain domain-neutral. It must not introduce political, voter-specific, or other application-specific semantics.

## Scope

V0.5 will add a thin orchestration API that composes the existing primitives:

1. create the import batch;
2. parse CSV text;
3. prepare and validate staging rows;
4. persist staged rows and diagnostics;
5. return a stable final result for ordinary import-quality outcomes;
6. record batch-level failures when parsing or staging cannot reach row persistence;
7. surface operational and programming failures as exceptions.

V0.5 will not add filesystem reads, upload handling, delimiter auto-detection, streaming, retry tooling, pagination, publication, arbitrary status mutation, or a new issue table.

## Existing Components Reused

The orchestrator will compose, not replace, these existing modules:

- `src/csv/parse-csv-records.ts`
  - owns CSV syntax and structural validation only;
- `src/ingestion/prepare-record-staging.ts`
  - owns schema/header validation, canonical transformation, record-ID extraction, and caller diagnostics;
- `src/ingestion/persist-record-staging.ts`
  - owns transactional row/issue persistence and the existing `RECEIVED -> VALIDATING -> VALIDATED/FAILED` persistence lifecycle;
- `src/db/imports.ts`
  - owns import creation and read/query operations.

No existing primitive will be folded into the orchestrator or made private.

## Public API

Add a new module, expected at:

`src/ingestion/run-record-import.ts`

Conceptual input shape:

```ts
export type RunRecordImportInput = {
  db: Queryable;
  importId: string;
  contract: RecordSchemaContract;
  csvText: string;
  transform: (row: Record<string, string>) => Record<string, unknown>;
  getRecordId: (
    row: Record<string, string>,
    canonical: Record<string, unknown>,
  ) => string | null;
  diagnose?: (
    row: Record<string, string>,
    canonical: Record<string, unknown>,
  ) => StagingDiagnostic[];
};
```

The API should return a structured result for ordinary import outcomes:

```ts
export type RunRecordImportResult = {
  importId: string;
  status: "VALIDATED" | "FAILED";
  summary: ImportSummary;
};
```

The exact exported type names may be adjusted during implementation if needed for consistency with existing repository conventions, but the semantics above are required.

The orchestrator should obtain the returned summary from the database after the terminal state has been committed so callers receive persisted state rather than a separately recomputed in-memory approximation.

## Lifecycle and Data Flow

### 1. Create the batch first

Every attempted import must create an `import_batch` before parsing or staging begins, whenever the database is reachable and the requested `importId` is available.

Initial state:

`RECEIVED`

This provides a durable audit record for malformed input that never reaches row staging.

A duplicate `importId` is the exception: batch creation itself fails because the identifier already exists, so no second audit record is created.

### 2. Parse CSV

The orchestrator passes `csvText` to `parseCsvRecords`.

If parsing succeeds, the resulting headers and rows continue to staging.

If parsing fails:

- record one batch-level issue with `row_number = NULL` and `record_id = NULL`;
- use severity `ERROR`;
- use issue code `CSV_PARSE_ERROR`;
- transition the import batch directly from `RECEIVED` to `FAILED`;
- return a normal `RunRecordImportResult` with status `FAILED`.

The parser's human-readable error text may be stored as issue detail. The orchestration contract must not require consumers to parse that text to determine failure type.

### 3. Prepare staging

The orchestrator passes parsed headers and rows into `prepareRecordStaging` unchanged.

Header/schema validation failures are ordinary input-quality failures. If staging fails because the supplied CSV headers violate the schema contract:

- record one batch-level `ERROR` issue;
- use issue code `SCHEMA_HEADER_ERROR`;
- leave `row_number` and `record_id` null;
- transition the batch directly to `FAILED`;
- return a normal `FAILED` result.

### 4. Callback exceptions during staging

Exceptions thrown by caller-supplied `transform`, `getRecordId`, or `diagnose` callbacks are programming/application failures rather than ordinary bad-input results.

Required behavior:

- stop staging immediately;
- do not persist any partially prepared rows;
- make a best-effort attempt to record one batch-level issue with code `STAGING_CALLBACK_ERROR`;
- make a best-effort attempt to mark the batch `FAILED`;
- rethrow the original callback error unchanged.

The orchestration layer must preserve the original exception as the thrown error. Failure to record the terminal audit state must not replace or hide the original callback exception.

### 5. Persist prepared staging rows

If CSV parsing and staging complete, call the existing `persistRecordStaging` API.

Prepared rows may contain row-level `ERROR` diagnostics. These are still persisted because the existing persistence layer records rows/issues and marks the import `FAILED` when errors exist.

Therefore:

- zero row-level errors -> existing persistence path ends in `VALIDATED`;
- one or more row-level errors -> existing persistence path ends in `FAILED`;
- warnings alone do not fail the import.

The orchestrator must not duplicate the row-status or issue-count rules already implemented in `persistRecordStaging`.

### 6. Return committed summary

After a normal terminal outcome, query `getImportSummary(importId)` and return it with the import ID and terminal status.

A successfully created and terminalized import should always have a summary. If the summary is unexpectedly missing, treat that as an operational invariant failure and throw.

## Batch-Level Failure Persistence

Reuse the existing `import_issue` table for failures that do not belong to a specific source row.

Batch-level issue representation:

- `row_number = NULL`
- `record_id = NULL`
- `severity = ERROR`

Required V0.5 codes:

- `CSV_PARSE_ERROR`
- `SCHEMA_HEADER_ERROR`
- `STAGING_CALLBACK_ERROR`
- `IMPORT_PERSISTENCE_ERROR`

No new `import_batch_issue` table will be created.

## Narrow Failure-Recording Primitive

V0.5 should add one narrowly scoped database helper for terminalizing a previously created batch when failure occurs before or outside `persistRecordStaging`.

Conceptually:

```ts
failImportBatch(db, {
  importId,
  issueCode,
  detail,
})
```

Required behavior:

- operate transactionally;
- accept only the intended transition from `RECEIVED` to `FAILED` for pre-staging failures, plus any explicitly required recovery transition identified during implementation;
- insert exactly one batch-level issue for the supplied failure;
- avoid exposing a generic arbitrary status setter;
- fail if the expected batch state is not present rather than silently mutating an unrelated lifecycle state.

If implementation requires separate helpers for normal pre-staging terminalization and best-effort recovery after a persistence transaction failure, they must remain narrow and lifecycle-specific rather than becoming a general status mutation API.

## Database and Infrastructure Failures

Database/infrastructure failures are exceptions, not ordinary `FAILED` import results.

If a database failure occurs after the batch was created, the orchestrator must make a best-effort attempt to:

1. record a batch-level `IMPORT_PERSISTENCE_ERROR` issue; and
2. mark the batch `FAILED`.

Then it must throw the original database error.

If the database is unavailable or the recovery write also fails, the orchestrator cannot guarantee a durable terminal record. In that case:

- preserve and throw the original operational error;
- do not claim the batch was successfully marked `FAILED`;
- do not substitute the recovery error for the original failure.

This is explicitly best-effort recovery, not a guarantee under total database failure.

## Duplicate Import IDs

Duplicate `importId` remains an exception using the existing stable behavior from `createImportBatch`.

The orchestrator must not convert duplicate IDs into a normal `FAILED` import result because no new batch can be created under the duplicate primary key.

## Error Classification

The orchestrator should classify failures by source, not by asking callers to parse human-readable messages.

Preferred implementation direction:

- CSV parser failures -> `CSV_PARSE_ERROR`;
- schema/header contract failures -> `SCHEMA_HEADER_ERROR`;
- caller callback exceptions -> `STAGING_CALLBACK_ERROR`;
- database/persistence failures -> `IMPORT_PERSISTENCE_ERROR`.

If existing APIs do not currently expose enough structure to distinguish header-contract failures from callback failures safely, V0.5 may introduce narrow typed error classes or stable error metadata at the relevant module boundary. It should not build brittle orchestration behavior around substring matching error messages.

Any such typed-error improvement must remain limited to classification needed by this orchestration feature.

## Atomicity Rules

The following atomicity requirements are mandatory:

- CSV/header failures persist no stage rows.
- Callback exceptions persist no partial stage rows.
- Prepared-row persistence remains governed by the transaction already inside `persistRecordStaging`.
- A failure inside that persistence transaction must roll back its staging writes according to existing behavior.
- Best-effort recovery after persistence failure occurs in a separate transaction because the failed staging transaction has already rolled back.

## Preservation Semantics

V0.5 must not change existing data-preservation rules:

- CSV field strings remain untrimmed and uncoerced by the parser;
- `rawSourceRow` remains lossless source data;
- `sourceRow` remains exclusively caller-transformer output;
- row-level diagnostics retain existing severity semantics;
- orchestration must not introduce hidden normalization.

Existing `persistRecordStaging` record-ID persistence behavior is out of scope unless a test demonstrates that orchestration cannot function correctly without changing it. Any such change would require an explicit design revision rather than a silent refactor.

## Testing Strategy

Implementation will follow TDD.

### Unit tests

Add focused tests for orchestration-level branching and failure classification, including:

- successful import returns `VALIDATED`;
- row-level error diagnostics return `FAILED` without throwing;
- warnings-only import returns `VALIDATED`;
- malformed CSV creates a batch-level `CSV_PARSE_ERROR` and returns `FAILED`;
- unsupported or missing schema headers create `SCHEMA_HEADER_ERROR` and return `FAILED`;
- callback exception records `STAGING_CALLBACK_ERROR` best-effort and rethrows the original error;
- duplicate import ID throws the existing duplicate error;
- unexpected missing summary throws an invariant/operational error.

### Integration tests

Using the existing transactional in-memory `Queryable` harness, verify:

- import batch is created before CSV parsing;
- pre-staging failures produce no stage rows;
- batch-level issues have null row/record identifiers;
- row-level errors still flow through existing `persistRecordStaging` behavior;
- failure-recording helper performs the correct lifecycle transition transactionally;
- callback exceptions do not persist partial rows;
- persistence transaction failure triggers best-effort recovery and rethrows the original error;
- recovery failure does not replace the original database error.

### Live PostgreSQL gate

Extend the existing PostgreSQL integration coverage to exercise at least:

- one successful end-to-end orchestrated import;
- one pre-staging failure with durable batch-level issue and `FAILED` status;
- one row-level validation failure persisted through the existing staging transaction.

The full existing verification suite must continue to pass.

## Acceptance Criteria

V0.5 is complete when:

1. callers can execute a complete CSV import through one supported function;
2. every non-duplicate import attempt creates an audit batch before parsing;
3. ordinary CSV/schema/row-quality failures return stable `FAILED` results rather than throwing;
4. callback, duplicate-ID, and infrastructure failures throw;
5. pre-staging failures are represented in the existing issue table as batch-level issues;
6. callback failures never persist partial rows;
7. database failures receive best-effort terminalization without hiding the original error;
8. no new database table or lifecycle status is introduced;
9. existing parser, staging, persistence, and query APIs remain independently usable;
10. all unit, integration, typecheck/build, and live PostgreSQL gates pass.

## Deferred Work

Explicitly deferred beyond V0.5:

- filesystem/path/file adapters;
- browser or HTTP upload adapters;
- streaming CSV ingestion;
- retry/resume controls;
- import cancellation;
- pagination and operational dashboards;
- publication/downstream release flows;
- arbitrary lifecycle administration;
- migration checksums;
- domain-specific record semantics.
