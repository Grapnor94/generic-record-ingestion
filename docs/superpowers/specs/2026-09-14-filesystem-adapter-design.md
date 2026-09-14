# V0.6 Filesystem Adapter Design

## Status

Approved in chat on 2026-09-14. This specification defines V0.6 of the domain-neutral generic record-ingestion framework.

## Goal

Add a supported filesystem-based import entry point that reads a local CSV file under a strict UTF-8 contract and feeds it into the existing V0.5 CSV orchestration pipeline without changing the public behavior of `runRecordImport(...)`.

V0.6 is intentionally narrow. It adds local file-path ingestion only. It does not add streaming, uploads, alternate encodings, delimiter detection, directory scanning, retries, publication, or domain-specific behavior.

## Public API

Add a new public function:

```ts
runRecordFileImport({
  db,
  importId,
  contract,
  filePath,
  transform,
  getRecordId,
  diagnose,
})
```

Proposed input type:

```ts
export type RunRecordFileImportInput = {
  db: Queryable;
  importId: string;
  contract: RecordSchemaContract;
  filePath: string;
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

Return type remains aligned with V0.5:

```ts
export type RunRecordImportResult = {
  importId: string;
  status: "VALIDATED" | "FAILED";
  summary: ImportSummary;
};
```

The existing `runRecordImport(...)` API remains source-compatible and continues to accept `csvText`.

## Architecture

Use a thin filesystem wrapper plus a shared internal orchestration helper.

The external paths become:

```text
runRecordImport(csvText)
  -> create import batch
  -> shared post-batch CSV pipeline

runRecordFileImport(filePath)
  -> create import batch
  -> read file bytes
  -> strict UTF-8 decode
  -> shared post-batch CSV pipeline
```

The shared post-batch pipeline owns the existing V0.5 behavior after a durable import batch already exists:

```text
CSV parse
  -> schema/header validation
  -> staging preparation
  -> persistence
  -> committed summary lookup
```

This avoids duplicate lifecycle logic while ensuring each import batch is created exactly once.

Do not expose a public `skipBatchCreation` flag or similar lifecycle escape hatch. The shared helper is internal-only.

## File Reading and Decoding Semantics

`runRecordFileImport(...)` accepts a filesystem path string. It reads the entire file as bytes, then decodes those bytes using strict/fatal UTF-8 semantics before parsing.

Implementation must not rely on a permissive UTF-8 conversion that silently replaces malformed byte sequences. Malformed UTF-8 must be rejected and handled as `FILE_READ_ERROR`.

The file adapter owns only file access and strict decoding into text. It does not:

- detect delimiters
- infer file formats
- trim or normalize source values
- interpret domain fields
- stream records
- accept caller-supplied `Buffer` or stream input
- support alternate encodings

Once text is decoded successfully, all CSV parsing and import semantics are delegated to the existing V0.5 pipeline.

## Durable Failure Semantics

Every non-duplicate file import attempt must create a durable import batch before file reading begins.

If the file cannot be read or cannot be decoded under the strict UTF-8 contract, terminalize the import as:

```text
status = FAILED
issue_code = FILE_READ_ERROR
severity = ERROR
row_number = NULL
record_id = NULL
field_key = NULL
```

No staging rows are inserted for a file-read or decode failure.

Expected input/file-access failures return a normal `FAILED` import result rather than throwing. This includes errors such as:

- path does not exist
- permission denied
- path cannot be opened as a readable file
- file bytes are not valid UTF-8

The issue detail should retain the underlying error message through the framework's existing stable error-detail conversion.

Unexpected programming defects remain exceptional and may throw.

No new lifecycle status is introduced. No database migration is required.

## Existing Error Semantics After a Successful File Read

After file text is available, V0.6 must preserve V0.5 behavior unchanged:

- malformed CSV -> `FAILED` + `CSV_PARSE_ERROR`, returned result
- unsupported/missing headers -> `FAILED` + `SCHEMA_HEADER_ERROR`, returned result
- row-level `ERROR` diagnostics -> persisted invalid rows + `FAILED`, returned result
- warnings-only -> `VALIDATED`, returned result
- staging callback exception -> best-effort `STAGING_CALLBACK_ERROR`, then rethrow original callback error unchanged
- persistence exception -> best-effort `IMPORT_PERSISTENCE_ERROR`, then rethrow original persistence error unchanged
- duplicate `importId` -> existing stable duplicate-import exception
- missing committed summary -> existing invariant exception

The filesystem wrapper must not reinterpret these downstream outcomes.

## Refactor Constraint

Refactor `runRecordImport(...)` only enough to share the post-batch pipeline.

The refactor must preserve:

- public function name and signature
- batch-creation timing for CSV-text imports
- all issue codes
- return-vs-throw behavior
- summary semantics
- callback cause identity
- persistence recovery behavior

This is an internal decomposition, not a V0.5 behavior change.

## Testing Strategy

Use TDD and preserve the existing verification gates.

Required coverage:

1. valid UTF-8 CSV file -> `VALIDATED` and same persisted raw/canonical behavior as `runRecordImport(...)`
2. missing file -> durable `FAILED` batch + one batch-level `FILE_READ_ERROR` + no staged rows
3. unreadable file -> durable `FAILED` batch + `FILE_READ_ERROR` + no staged rows
4. malformed UTF-8 bytes -> durable `FAILED` batch + `FILE_READ_ERROR`
5. readable malformed CSV -> existing `CSV_PARSE_ERROR`
6. readable CSV with invalid headers -> existing `SCHEMA_HEADER_ERROR`
7. readable CSV with row-level errors -> existing invalid-row + `FAILED` semantics
8. warnings-only file import -> `VALIDATED`
9. staging callback failure -> exact original callback error is rethrown and existing recovery semantics remain intact
10. persistence failure -> exact original persistence error is rethrown and existing recovery semantics remain intact
11. duplicate import ID -> existing duplicate-import exception
12. regression coverage proving `runRecordImport(...)` behavior is unchanged

Add live PostgreSQL coverage for at least:

- one successful filesystem import
- one file-read failure with durable batch-level `FILE_READ_ERROR`
- one downstream row-level failure through the filesystem entry point

## Non-Goals

V0.6 does not add:

- streams
- large-file chunking
- memory-pressure/file-size policy
- uploads or HTTP inputs
- directories or globbing
- file watching
- retries
- alternate character encodings
- delimiter sniffing
- non-CSV formats
- publication workflow
- domain-specific schemas or semantics

These remain future concerns and should not be abstracted prematurely.

## Success Criteria

V0.6 is complete when:

- callers can import a local CSV file through `runRecordFileImport(...)`
- every non-duplicate attempt creates a durable batch before file reading
- file-access and strict UTF-8 failures are auditable as `FAILED` + `FILE_READ_ERROR`
- all downstream CSV/staging/persistence behavior remains identical to V0.5
- the existing `runRecordImport(...)` API remains unchanged
- unit/integration tests and live PostgreSQL verification pass at the exact feature head
- README and verification documentation describe the filesystem entry point and its failure semantics
