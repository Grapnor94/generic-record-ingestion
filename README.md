# Generic Record Ingestion

A standalone, domain-neutral TypeScript ingestion framework for structured record imports.

It provides:
- UTF-8 comma-separated CSV parsing with strict structural checks;
- required/optional header validation with unknown-column rejection;
- lossless raw-row copies kept separate from canonical transformer output;
- caller-supplied record-ID extraction and diagnostics;
- generic missing/duplicate record-ID validation;
- deterministic warning/error staging reports;
- transactional persistence of canonical and raw JSON rows;
- warning/error persistence where warnings remain non-blocking;
- import lifecycle/query APIs;
- a supported end-to-end CSV import orchestration API.

## Scope

Production code and tests contain no political-domain attributes or mappings. The framework is intentionally generic.

## Commands

```bash
npm run typecheck
npm test
npm run test:integration
npm run verify
npm run db:migrate
```

Tests use Node's built-in test runner. Integration tests use a transactional in-memory `Queryable` harness to validate orchestration, SQL call sequencing, warning/error semantics, and rollback behavior.

## End-to-end import orchestration

V0.5 adds `runRecordImport(...)` in `src/ingestion/run-record-import.ts` as the supported application-level entry point for already-decoded CSV text:

```ts
const result = await runRecordImport({
  db,
  importId: "import-2026-09-14-001",
  contract,
  csvText,
  transform,
  getRecordId,
  diagnose,
});
```

The orchestrator composes the existing primitives rather than replacing them. Its normal flow is:

1. create the `import_batch` in `RECEIVED` state;
2. parse CSV text;
3. validate headers and prepare canonical staging rows;
4. persist stage rows and diagnostics transactionally;
5. read the committed import summary;
6. return `{ importId, status, summary }`.

Ordinary input-quality failures are durable results rather than thrown exceptions. Malformed CSV is stored as a batch-level `CSV_PARSE_ERROR`; unsupported/missing schema headers are stored as `SCHEMA_HEADER_ERROR`; row-level diagnostics continue through the existing persistence path and can end the batch in `FAILED`. Batch-level issues use `row_number = NULL` and `record_id = NULL`.

Caller callback failures from `transform`, `getRecordId`, or `diagnose` are treated as programming/application failures. The framework makes a best-effort attempt to record `STAGING_CALLBACK_ERROR`, persists no partial stage rows, and rethrows the original callback error. Persistence/database failures similarly receive best-effort `IMPORT_PERSISTENCE_ERROR` terminalization while preserving the original thrown database error. Duplicate import IDs keep the existing stable exception behavior.

No new lifecycle status or issue table is introduced by V0.5. Pre-staging terminalization is deliberately narrow: only `RECEIVED -> FAILED` is supported outside the existing persistence transaction.

## CSV adapter

`src/csv/parse-csv-records.ts` exposes a focused synchronous adapter for already-decoded UTF-8, comma-separated CSV text:

```ts
const parsed = parseCsvRecords(csvText);

const prepared = prepareRecordStaging({
  contract,
  headers: parsed.headers,
  rows: parsed.rows,
  transform,
  getRecordId,
  diagnose,
});
```

V0.4 supports standard quoted CSV fields, commas inside quoted fields, escaped double quotes, LF and CRLF line endings, empty fields, embedded quoted newlines, and an initial UTF-8 BOM. Decoded header and field strings are preserved without trimming or type coercion.

The adapter owns CSV syntax and row-width validation only. The existing staging layer remains authoritative for schema-contract/header validation, canonical transformation, record-ID validation, and caller-supplied diagnostics. The CSV adapter does not perform lifecycle mutation or database persistence.

V0.4 intentionally does not support TSV or pipe-delimited input, delimiter auto-detection, spreadsheet formats, non-UTF-8 transcoding, streaming, filesystem reads, or HTTP upload handling.

## Database bootstrap and migrations

The package owns these PostgreSQL tables:

- `import_batch`
- `import_stage_row`
- `import_issue`
- `schema_migration`

Apply all pending migrations with:

```bash
npm run db:migrate
```

The command uses `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, and `PGDATABASE`. Migration files live in `db/migrations`, are applied in lexical filename order, and are forward-only. Applied filenames are recorded in `schema_migration`; rerunning the command skips migrations already present in the ledger.

Committed migrations are immutable. Introduce schema changes with a new migration file rather than editing an already-published migration.

## Import lifecycle/query API

The database service layer in `src/db/imports.ts` exposes framework-neutral lifecycle/query functions:

- `createImportBatch` creates a new `RECEIVED` import batch;
- `failImportBatch` transactionally records one batch-level error while allowing only `RECEIVED -> FAILED`;
- `getImportBatch` reads batch metadata/status;
- `listImportRows` returns staged rows in deterministic row-number order with an optional validation-status filter;
- `listImportIssues` returns diagnostics in deterministic row/issue order with optional severity and row filters;
- `getImportSummary` returns row-state and diagnostic counts without multiplying aggregates.

Lifecycle mutation remains deliberately narrow. `persistRecordStaging` owns the validation transition `RECEIVED -> VALIDATING -> VALIDATED | FAILED`; there is no generic arbitrary status setter. Missing batch/summary lookups return `null`, while missing row/issue matches return empty arrays.

## Live PostgreSQL verification

The live gate starts from an empty isolated PostgreSQL schema, applies the repository migrations with the same migration runner used by `npm run db:migrate`, and then exercises the real persistence and orchestration APIs. It checks:

- clean-schema bootstrap creates all four package-owned tables;
- migrations apply in lexical order and are recorded in `schema_migration`;
- rerunning migrations is idempotent and skips applied filenames;
- `raw_source_row` is nullable `jsonb`;
- canonical and raw JSON remain distinct;
- warnings remain non-blocking and rows become `VALID`;
- blocking errors make rows `INVALID` and the batch `FAILED`;
- an injected database failure rolls back the batch status and all staged writes;
- a successful two-row CSV import completes through `runRecordImport`;
- a malformed CSV attempt is durably terminalized with a batch-level issue;
- a row-level validation error persists an `INVALID` row and fails the batch.

Run it where PostgreSQL is available:

```bash
PGHOST=127.0.0.1 \
PGPORT=5432 \
PGUSER=postgres \
PGPASSWORD=postgres \
PGDATABASE=postgres \
npm run test:postgres
```

The included `.github/workflows/postgres-integration.yml` provisions PostgreSQL 18 as a disposable service and runs both the normal verification suite and this live gate.
