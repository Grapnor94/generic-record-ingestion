# Generic Record Ingestion

A domain-neutral TypeScript framework for bounded CSV ingestion and transactional PostgreSQL staging. It preserves raw source rows separately from canonical data, validates headers and record IDs, retains diagnostics and source provenance, and supports explicit recovery of interrupted attempts.

## Supported V0.8 package contract

Import from `generic-record-ingestion`. The root exposes preparation, durable start/resume, attempt/summary reads, bounded row/issue pages, migrations, limits, errors, and their TypeScript contracts. These are intentional V0.8 contracts, not a claim of mature semantic-versioning guarantees. The package remains private at version `0.1.0`; no registry publication is implied. Node.js 22+ and ESM are supported; no CommonJS interface is promised.

The exports map blocks deep paths, including `generic-record-ingestion/db/imports.js` and `generic-record-ingestion/dist/db/imports.js`, with `ERR_PACKAGE_PATH_NOT_EXPORTED`. Repository helpers, SQL mutations, transaction primitives, migration discovery/ledger helpers, cursor encoders, and raw database DTOs are internal.

## Preparation without a database

Preparation accepts parsed string records. It does not read files or acquire a database connection, and its capacity remains caller-managed. Importing the root and using preparation does not load the CSV parser or `pg`.

```ts
import { prepareRecordStaging, type RecordSchemaContract } from "generic-record-ingestion";

const contract: RecordSchemaContract = {
  schemaVersion: "EXAMPLE_V1",
  requiredHeaders: ["record_id", "label"],
};
const prepared = prepareRecordStaging({
  contract,
  headers: ["record_id", "label"],
  rows: [{ record_id: "R-1", label: "  Example  " }],
  transform: row => ({ label: row.label.trim() }),
  getRecordId: row => row.record_id,
});
// prepared.rows[0].rawSourceRow.label === "  Example  "
// prepared.rows[0].sourceRow.label === "Example"
```

Unknown or missing required headers throw `UnsupportedRecordSchemaError`. Callback failures throw `RecordStagingCallbackError` with the row number and original cause. `StagingDiagnostic` severity is `ERROR` or `WARNING`; only errors make `StagingReport.canProceedToPersistence` false. Callbacks share a working row separate from the raw snapshot. `PreparedRecord`, `StagingReport`, and `StagingDiagnostic` are root-exported types alongside `RecordSchemaContract`.

## Durable start with a client or pool

Install the package's runtime dependencies for durable ingestion. The application owns its PostgreSQL client or pool and its lifetime. Pass the explicit `PostgresDatabase` discriminator; a `CLIENT` must be a connected dedicated client, not a pool disguised as a client. Do not share that client concurrently or call these workflows inside an application-owned transaction. A `POOL` acquires and releases one dedicated connection for each operation, including failures.

```ts
import pg from "pg";
import { runMigrations, startDurableImport } from "generic-record-ingestion";

const client = new pg.Client(); // pg reads the application's PG* environment
await client.connect();
try {
  const database = { kind: "CLIENT" as const, client };
  await runMigrations(database);
  const result = await startDurableImport({
    database,
    importId: "example-client-001",
    contract: { schemaVersion: "EXAMPLE_V1", requiredHeaders: ["record_id", "label"] },
    source: { kind: "CSV_TEXT", text: "record_id,label\nR-1,Example\n" },
    transform: row => ({ label: row.label.trim() }),
    getRecordId: row => row.record_id,
  });
  console.log(result.status, result.summary);
} finally {
  await client.end();
}
```

```ts
import pg from "pg";
import { runMigrations, startDurableImport } from "generic-record-ingestion";

const pool = new pg.Pool({ max: 4 });
try {
  const database = { kind: "POOL" as const, pool };
  await runMigrations(database);
  const result = await startDurableImport({
    database,
    importId: "example-file-001",
    contract: { schemaVersion: "EXAMPLE_V1", requiredHeaders: ["record_id", "label"] },
    source: { kind: "LOCAL_FILE", filePath: "/data/records.csv" },
    transform: row => ({ label: row.label.trim() }),
    getRecordId: row => row.record_id,
    diagnose: () => [],
    limits: { maxDataRows: 10_000 },
  });
  console.log(result.status, result.summary.rowCount);
} finally {
  await pool.end();
}
```

`DurableImportInput` uses `database` and an `ImportSource` with either CSV_TEXT `text` or LOCAL_FILE `filePath`. Start creates one `RECEIVED` attempt before source processing. Staging claims it through `VALIDATING` and commits rows, diagnostics, and the final `VALIDATED` or `FAILED` state atomically. Warnings remain nonblocking; row errors produce `FAILED`. `RunRecordImportResult` is `{ importId, status, summary }` with the committed `ImportSummary`.

CSV is UTF-8, comma-separated, and supports quoted fields, escaped quotes, LF/CRLF, embedded quoted newlines, empty fields, and an initial BOM. Duplicate physical headers and inconsistent row widths are rejected. Local files use strict fatal UTF-8 decoding. Processing remains whole-file/whole-batch, without streaming, chunked persistence, alternate encodings, HTTP adapters, or source storage.

## Finite limits and measured envelope

| Limit | Default | Maximum |
| --- | ---: | ---: |
| `maxSourceBytes` | 20,074,811 | 80,298,686 |
| `maxDataRows` | 25,000 | 100,000 |

`DEFAULT_INGESTION_LIMITS` and `MAX_INGESTION_LIMITS` are frozen root constants. Omitted fields use defaults. Overrides must be positive safe integers within the maximums; zero, negative, fractional, nonfinite, and above-maximum values throw `INVALID_INGESTION_LIMIT` before an attempt is created. There is no unbounded mode. Both limits apply independently, with CSV text counted as UTF-8 bytes. Local files are checked by size before reading and by actual bytes after reading. Exceeding source or data-row bounds creates a durable failed result with no staged rows.

The [operating-envelope evidence](docs/v0.8-bounded-operating-envelope.md) records synthetic parse/preparation characterization through 100,000 rows. This is not a database throughput or universal memory guarantee. Callback allocations, diagnostics, database persistence, and concurrent imports require their own capacity planning; lower application limits are supported.

## Explicit recovery and provenance

Use `getImportAttempt(database, importId)` to inspect a prior attempt. It returns `ImportBatch | null`; `getImportSummary` returns `ImportSummary | null`. Only `RECEIVED` can resume. `VALIDATING` is never reclaimed, and `VALIDATED` and `FAILED` are terminal. Duplicate start always throws `IMPORT_ALREADY_EXISTS`.

```ts
import { getImportAttempt, resumeDurableImport, type DurableImportInput } from "generic-record-ingestion";

async function recover(input: DurableImportInput) {
  const attempt = await getImportAttempt(input.database, input.importId);
  if (attempt?.status === "RECEIVED") {
    return resumeDurableImport(input);
  }
  return null;
}
```

The caller must retain and resupply the source, schema contract, and callbacks. The framework stores no source artifact and performs no scheduling or automatic retry. Resume applies the same bounds and compares schema version plus every established non-null provenance field before staging. Missing provenance may be completed. A conflict throws `SOURCE_PROVENANCE_MISMATCH`; a competing resume can throw `IMPORT_NOT_RESUMABLE` even after the inspection above succeeds.

`ImportSourceMetadata`, included in attempts and summaries, contains `sourceKind`, `sourceName`, `sourceSizeBytes`, `sourceSha256`, and `sourcePath`. Size and SHA-256 describe the original bytes, including BOM and line endings. File attempts retain the basename; supported entry points store no full path in `sourcePath`. Unreadable files can retain partial provenance. Legacy fields may be null. Error detail text may include underlying filesystem error information. Content checksums are diagnostic identity, not deduplication: different attempt IDs may import identical content.

## Bounded pagination

```ts
import { getImportRowsPage, getImportIssuesPage, type PostgresDatabase } from "generic-record-ingestion";

async function readPages(database: PostgresDatabase, importId: string) {
  let cursor: string | undefined;
  do {
    const page = await getImportRowsPage(database, importId, {
      pageSize: 100, status: "VALID", cursor,
    });
    console.log(page.items);
    cursor = page.nextCursor ?? undefined;
  } while (cursor !== undefined);
  return getImportIssuesPage(database, importId, { pageSize: 100, severity: "WARNING" });
}
```

`Page<T>` contains `items` and `nextCursor`; null means the end. Sizes range from 1 to `MAX_PAGE_SIZE` (1,000), defaulting to `DEFAULT_PAGE_SIZE` (100). Row pages sort by row number; issue pages sort by row number with batch-level null rows first, then issue ID. Issue filters also support `rowNumber`. Treat cursors as opaque and retain the same attempt and filters between pages. Row and issue cursors are not interchangeable. These are position reads, not snapshot isolation across calls; prefer terminal attempts when traversing stable results.

## Errors and migration reporting

Expected operation/configuration failures throw the root `FrameworkError`. Branch on its `FrameworkErrorCode`-typed `code`, not its message:

| Code | Meaning |
| --- | --- |
| `INVALID_INGESTION_LIMIT` | Invalid durable limit override |
| `INVALID_PAGE_SIZE` | Page size outside the integer range |
| `INVALID_CURSOR` | Malformed or wrong-kind cursor |
| `IMPORT_ALREADY_EXISTS` | Start reused an attempt ID |
| `IMPORT_NOT_FOUND` | Resume could not find the attempt |
| `IMPORT_NOT_RESUMABLE` | Attempt is not RECEIVED, including a lost claim |
| `SOURCE_PROVENANCE_MISMATCH` | Resupplied source/schema conflicts with stored identity |
| `MIGRATION_CHECKSUM_MISMATCH` | A checksummed migration's file bytes changed |

Expected source/content failures return a durable `FAILED` result: issue codes include `CSV_PARSE_ERROR`, `DUPLICATE_HEADER`, `SCHEMA_HEADER_ERROR`, `FILE_READ_ERROR`, `SOURCE_SIZE_LIMIT_EXCEEDED`, and `ROW_LIMIT_EXCEEDED`, plus row diagnostics. Durable callback failures rethrow the original cause after best-effort `STAGING_CALLBACK_ERROR` terminalization. Unexpected persistence or provenance-write failures also preserve the original exception after best-effort failure recording. Not every thrown error is a `FrameworkError`.

`runMigrations(database, options?)` uses bundled SQL files by default. `RunMigrationsOptions.migrationsDir` selects an alternate directory. Migrations run forward in filename order, on one dedicated connection under an advisory lock, with each migration and its ledger entry in one transaction. Exact file bytes are SHA-256 checked before new pending migrations execute.

`MigrationResult` reports `applied`, `verified`, and `legacyUnverified` filenames. A legacy ledger row with no checksum remains unverified on every run; the runner does not infer historical checksums. Review `legacyUnverified` explicitly. The package owns `import_batch`, `import_stage_row`, `import_issue`, and `schema_migration`. `npm run db:migrate` uses the normal `PG*` environment.

## Verification and synthetic example

```sh
npm run typecheck
npm run verify
npm run test:postgres
npm run example:inventory
```

`npm run verify` covers unit, in-memory integration, packed JavaScript/TypeScript, and runnable-example tests. `npm run test:postgres` requires reachable PostgreSQL and `psql` on PATH; configure `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, and `PGDATABASE`. The live suite includes a packed external consumer that imports only the root API, applies migrations, starts and resumes attempts, paginates rows/issues, and closes its pool. GitHub Actions provisions PostgreSQL 18 for these gates.

`npm pack` builds JavaScript and declarations. Packed tests extract the real archive into temporary consumers, with no source links or registry installation. Preparation tests intentionally have no runtime dependencies; the live packed consumer copies installed runtime dependencies. Tests need Node, npm, development TypeScript, and `tar` on PATH.

The inventory example packs the private library and runs `examples/inventory.mjs` using the root import. It demonstrates clean, warning, invalid-quantity, and duplicate-ID preparation scenarios without database writes. Raw whitespace and legacy codes remain in raw data; canonical data contains the caller's normalized item name, quantity, and warehouse. It is not a scale benchmark.
