# V0.7 Import Provenance Design

Date: 2026-09-14
Status: Frozen / approved
Baseline: V0.6 main at `14e088864b3f78af3914e084652ab14dce5accb5`

## Goal

Add durable, domain-neutral source provenance to every import attempt without changing the established CSV parsing, staging, validation, or lifecycle semantics.

V0.7 makes source content identifiable and auditable while preserving `importId` as the identity of an individual import attempt.

## Database model

Add nullable provenance columns directly to `import_batch`:

- `source_kind`
- `source_name`
- `source_size_bytes`
- `source_sha256`
- `source_path`

The migration must remain compatible with existing rows; all new fields are nullable.

`source_sha256` is informational and MUST NOT have a uniqueness constraint. Multiple import attempts may legitimately contain identical source content.

The one-source-per-import model remains authoritative for V0.7. A separate provenance/source table is intentionally deferred until a real one-to-many source requirement exists.

## Identity semantics

`importId` remains the lifecycle identity of an import attempt.

`source_sha256` identifies source content. Identical SHA-256 values do not imply duplicate import attempts and do not prevent ingestion.

Initial source kinds are:

- `CSV_TEXT`
- `LOCAL_FILE`

## CSV text provenance

`runRecordImport(...)` remains source-compatible and continues accepting decoded CSV text.

Before CSV parsing/staging, it will:

1. encode `csvText` as UTF-8 bytes;
2. calculate the exact UTF-8 byte length;
3. calculate SHA-256 from those bytes;
4. persist provenance on the already-created import batch;
5. continue through the existing post-batch pipeline.

For ordinary text imports:

- `source_kind = CSV_TEXT`
- `source_name = NULL`
- `source_size_bytes = exact UTF-8 byte length`
- `source_sha256 = lowercase SHA-256 hex digest`
- `source_path = NULL`

Multibyte Unicode characters therefore count by encoded byte length rather than JavaScript string length.

## Local filesystem provenance

`runRecordFileImport(...)` continues accepting a local filesystem path and creates the durable import batch before reading the file.

After a successful file read and before UTF-8 decoding/parsing, it will:

1. retain the original bytes already read by the adapter;
2. calculate byte size directly from those bytes;
3. calculate SHA-256 directly from those bytes;
4. derive `source_name` from the path basename;
5. persist the provenance;
6. perform the existing strict fatal UTF-8 decode;
7. delegate to the existing post-batch CSV pipeline.

For successfully read local files:

- `source_kind = LOCAL_FILE`
- `source_name = basename of the supplied path`
- `source_size_bytes = exact file byte length`
- `source_sha256 = lowercase SHA-256 hex digest`
- `source_path = NULL` by default

The full machine-local path is deliberately not persisted by default.

## Filesystem read failures

Every non-duplicate filesystem import still creates its durable batch before attempting the read.

If the file cannot be read, source bytes are unavailable. The batch records partial provenance:

- `source_kind = LOCAL_FILE`
- `source_name = basename of the intended path`
- `source_size_bytes = NULL`
- `source_sha256 = NULL`
- `source_path = NULL`

The existing durable `FILE_READ_ERROR` behavior remains unchanged: the batch terminates as `FAILED`, the issue is batch-level, and no staged rows are created.

## Provenance timing and downstream failures

Once source bytes/text are available, complete provenance is persisted before CSV parsing, schema/header validation, row staging, or downstream persistence.

Consequently provenance survives ordinary downstream failure states, including:

- malformed CSV / `CSV_PARSE_ERROR`;
- unsupported or missing headers / `SCHEMA_HEADER_ERROR`;
- row-level validation errors;
- staging callback failures;
- persistence failures where best-effort terminalization succeeds.

Existing V0.6 return-vs-throw and error-authority semantics remain unchanged.

## Architecture

Introduce a small shared, domain-neutral provenance helper responsible for UTF-8 byte conversion/measurement and SHA-256 calculation.

Do not introduce a generic input/source adapter hierarchy. The existing public APIs remain distinct:

- `runRecordImport(...)` for CSV text;
- `runRecordFileImport(...)` for local filesystem CSV.

The shared post-batch parsing/staging pipeline remains authoritative for downstream behavior.

Provenance persistence should be narrow and explicit rather than bundled into arbitrary lifecycle status mutation.

## Query surface

Extend `ImportBatch` and `getImportBatch(...)` to expose all five provenance fields.

Extend `ImportSummary` and `getImportSummary(...)` with the same provenance fields so a caller can inspect terminal import provenance without an additional batch query.

Nullable database fields map to nullable TypeScript values. `source_size_bytes` must be represented safely for the supported size range and tested against PostgreSQL behavior.

## Compatibility

V0.7 must preserve:

- existing V0.6 public entry-point signatures unless an additive optional field is explicitly required by implementation;
- existing import lifecycle statuses and transition ownership;
- existing CSV parsing semantics;
- existing schema-contract behavior;
- raw/canonical staging separation;
- diagnostic behavior;
- duplicate `importId` behavior;
- callback error identity;
- persistence error authority;
- strict fatal UTF-8 filesystem decoding.

No existing import is invalidated by the provenance migration.

## Verification requirements

Tests must cover at minimum:

1. deterministic SHA-256 calculation;
2. exact UTF-8 byte counts, including multibyte characters;
3. complete provenance for CSV text imports;
4. complete provenance for successfully read local files;
5. filename basename extraction without persisted full path;
6. acceptance of multiple imports with identical SHA-256 content;
7. provenance retention after malformed CSV;
8. provenance retention after schema/header failure;
9. provenance retention after row-level failure;
10. partial provenance on filesystem read failure;
11. malformed UTF-8 file provenance based on successfully read original bytes;
12. migration compatibility with pre-V0.7 import rows;
13. query API exposure through both batch and summary reads;
14. live PostgreSQL persistence and retrieval;
15. full V0.6 regression suite.

The final exact-head feature-branch CI gate and post-merge `main` CI gate remain required release gates.

## Explicit exclusions

V0.7 does not add:

- checksum-based rejection or deduplication;
- uniqueness constraints on source checksums;
- full source-path persistence by default;
- multiple source files per import;
- provenance-history/source tables;
- file modification timestamps;
- MIME detection;
- alternate hash algorithms;
- streaming or chunked hashing;
- streaming ingestion;
- file-size/memory policy;
- cloud, HTTP, or upload sources;
- alternate encodings;
- delimiter detection;
- non-CSV formats;
- domain-specific metadata.

## Frozen decisions

1. Provenance applies to both text and filesystem imports.
2. SHA-256 is calculated automatically from the actual imported bytes.
3. UTF-8 text byte size is encoded byte length, not string length.
4. Filesystem source names use basename only.
5. `source_path` remains nullable and is not populated by default.
6. Duplicate content is allowed.
7. Provenance is persisted before downstream parsing/staging once bytes are available.
8. File-read failures retain partial provenance but cannot claim byte size or checksum.
9. Provenance fields live directly on `import_batch` in V0.7.
10. A small shared provenance helper is preferred over duplicate hashing logic or a generic adapter abstraction.
11. Batch and summary query surfaces expose provenance.
12. V0.6 downstream semantics remain authoritative unless this specification explicitly changes them.
