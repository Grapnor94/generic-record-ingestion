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
- durable source provenance for each import attempt;
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

Filesystem imports remain local-filesystem and whole-file only. They do not add streaming/chunking, upload or HTTP handling, cloud-storage adapters, directory/glob imports, file watching, retries, alternate encodings, delimiter detection, non-CSV formats, or a CLI. `Buffer`/byte data is not exposed in the public API.

## V0.7 import provenance

Migration `0002_add_import_provenance.sql` adds five nullable columns to `import_batch`. Existing rows retain NULL provenance. `ImportBatch` and `ImportSummary`, including summaries returned by both import entry points, expose:

| Field | Type | Meaning |
| --- | --- | --- |
| `sourceKind` | `"CSV_TEXT" \| "LOCAL_FILE" \| null` | Source category |
| `sourceName` | `string \| null` | File basename; NULL for CSV text |
| `sourceSizeBytes` | `number \| null` | Exact original byte count |
| `sourceSha256` | `string \| null` | Lowercase SHA-256 hex digest |
| `sourcePath` | `string \| null` | Reserved field; both entry points store NULL |

CSV text imports UTF-8 encode the supplied string and persist complete provenance with the initial RECEIVED batch, before parsing or staging. Multibyte characters count by encoded bytes, not JavaScript string length.

Filesystem imports persist LOCAL_FILE and the basename before reading. After a successful read they hash and measure the original bytes, then persist that metadata while RECEIVED, before strict UTF-8 decoding. A BOM and original line endings contribute to the checksum. Unreadable files retain partial provenance with NULL size/hash; malformed UTF-8 retains complete byte provenance. Both still use FILE_READ_ERROR.

Provenance survives malformed CSV, header errors, row errors, and callback/persistence failures. An unexpected database failure while updating filesystem content metadata triggers best-effort FAILED terminalization with IMPORT_PROVENANCE_ERROR and rethrows the original exception, even if recovery also fails. Initial batch-creation failures retain existing exception behavior.

`importId` remains import-attempt identity. Identical content under different IDs is accepted; SHA-256 provides diagnostic provenance only. There are no checksum lookup APIs, indexes, uniqueness constraints or deduplication.

`source_size_bytes` uses PostgreSQL bigint constrained to 0 through 9007199254740991, so batch and summary values map exactly to JavaScript numbers. NULL is accepted. The database also constrains source kinds and 64-character lowercase hexadecimal digests. The supported import entry points do not store full paths in provenance; existing error-detail behavior is unchanged.

## CSV adapter

`src/csv/parse-csv-records.ts` exposes a focused synchronous adapter for decoded UTF-8, comma-separated CSV text. It supports standard quoted fields, commas inside quoted fields, escaped double quotes, LF/CRLF line endings, empty fields, embedded quoted newlines, and an initial UTF-8 BOM. Decoded header and field strings are preserved without trimming or type coercion.

The CSV adapter owns syntax and row-width validation only. The staging layer remains authoritative for schema-contract/header validation, canonical transformation, record-ID validation, and caller-supplied diagnostics.

## Database bootstrap and migrations

The package owns `import_batch`, `import_stage_row`, `import_issue`, and `schema_migration`. Apply pending migrations with `npm run db:migrate`. The command uses `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, and `PGDATABASE`. Migrations are forward-only, applied in lexical filename order, and recorded in `schema_migration`.

## Import lifecycle/query API

The service layer in `src/db/imports.ts` exposes `createImportBatch`, `failImportBatch`, `getImportBatch`, `listImportRows`, `listImportIssues`, and `getImportSummary`. `createImportBatch` accepts optional provenance fields. `updateImportSourceContentMetadata(db, { importId, sourceSizeBytes, sourceSha256 })` updates only byte size and digest, and rejects missing batches or batches outside RECEIVED. Lifecycle mutation remains narrow: `persistRecordStaging` owns `RECEIVED -> VALIDATING -> VALIDATED | FAILED`, while pre-staging terminalization supports only `RECEIVED -> FAILED`.

## Live PostgreSQL verification

The GitHub Actions live gate provisions PostgreSQL 18 and exercises migrations, persistence, query APIs, text orchestration, and filesystem orchestration. Provenance coverage includes migration compatibility, constraints, safe bigint mapping, duplicate content, RECEIVED-only updates, byte-preserving file metadata, malformed UTF-8, unreadable files, and database-triggered metadata failure.

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
