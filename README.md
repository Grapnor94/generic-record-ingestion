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
- supported end-to-end CSV-text and local-filesystem import entry points.

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

`runRecordImport(...)` in `src/ingestion/run-record-import.ts` is the supported application-level entry point for already-decoded CSV text:

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

The orchestrator creates the `import_batch` in `RECEIVED`, parses CSV text, validates/prepares rows, persists stage rows and diagnostics transactionally, reads the committed summary, and returns `{ importId, status, summary }`.

Ordinary input-quality failures are durable results rather than thrown exceptions. Malformed CSV is stored as `CSV_PARSE_ERROR`; unsupported/missing schema headers as `SCHEMA_HEADER_ERROR`; row-level diagnostics continue through persistence and can end the batch in `FAILED`. Callback and persistence failures retain the V0.5 best-effort terminalization and original-error rethrow semantics.

## Filesystem adapter

V0.6 adds `runRecordFileImport(...)` in `src/ingestion/run-record-file-import.ts` for a local CSV filesystem path:

```ts
const result = await runRecordFileImport({
  db,
  importId: "import-2026-09-14-file-001",
  contract,
  filePath: "/data/records.csv",
  transform,
  getRecordId,
  diagnose,
});
```

Every non-duplicate file attempt creates its durable import batch before reading the file. The adapter reads the whole file into memory, then performs strict UTF-8 decoding with fatal error handling. A missing/unreadable file or malformed UTF-8 is durably terminalized as a batch-level `FILE_READ_ERROR`, returns a normal `FAILED` result, and creates no staged rows.

Once decoding succeeds, the file entry point delegates to the same post-batch CSV pipeline as `runRecordImport(...)`. Consequently malformed CSV, header/schema errors, row diagnostics, warnings, callback failures, persistence failures, duplicate IDs, and summary invariants retain the existing text-import semantics.

V0.6 is deliberately local-filesystem and whole-file only. It does not add streaming/chunking, upload or HTTP handling, cloud-storage adapters, directory/glob imports, file watching, retries, alternate encodings, delimiter detection, non-CSV formats, persisted file provenance/checksums, or a CLI. `Buffer`/byte data is not exposed in the public API.

## CSV adapter

`src/csv/parse-csv-records.ts` exposes a focused synchronous adapter for decoded UTF-8, comma-separated CSV text. It supports standard quoted fields, commas inside quoted fields, escaped double quotes, LF/CRLF line endings, empty fields, embedded quoted newlines, and an initial UTF-8 BOM. Decoded header and field strings are preserved without trimming or type coercion.

The CSV adapter owns syntax and row-width validation only. The staging layer remains authoritative for schema-contract/header validation, canonical transformation, record-ID validation, and caller-supplied diagnostics.

## Database bootstrap and migrations

The package owns `import_batch`, `import_stage_row`, `import_issue`, and `schema_migration`. Apply pending migrations with `npm run db:migrate`. The command uses `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, and `PGDATABASE`. Migrations are forward-only, applied in lexical filename order, and recorded in `schema_migration`.

## Import lifecycle/query API

The service layer in `src/db/imports.ts` exposes `createImportBatch`, `failImportBatch`, `getImportBatch`, `listImportRows`, `listImportIssues`, and `getImportSummary`. Lifecycle mutation remains narrow: `persistRecordStaging` owns `RECEIVED -> VALIDATING -> VALIDATED | FAILED`, while pre-staging terminalization supports only `RECEIVED -> FAILED`.

## Live PostgreSQL verification

The GitHub Actions live gate provisions PostgreSQL 18 and exercises migrations, persistence, query APIs, text orchestration, and filesystem orchestration. Filesystem coverage includes a successful CSV file, a durable missing-file `FILE_READ_ERROR`, and downstream row-level validation failure.

Run it where PostgreSQL is available:

```bash
PGHOST=127.0.0.1 \
PGPORT=5432 \
PGUSER=postgres \
PGPASSWORD=postgres \
PGDATABASE=postgres \
npm run test:postgres
```

The included `.github/workflows/postgres-integration.yml` provisions PostgreSQL 18 as a disposable service and runs both `npm run verify` and the live PostgreSQL gate.
