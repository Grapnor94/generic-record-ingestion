# V0.7 Import Provenance Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans to implement this plan task-by-task with explicit RED/GREEN checkpoints.

**Goal:** Retain diagnostic source provenance for each import attempt without altering V0.6 lifecycle behavior.

**Architecture:** Nullable columns on import_batch; a small byte hashing helper; optional provenance at batch creation; a RECEIVED-only content metadata update for filesystem imports. Existing post-batch staging remains unchanged.

**Tech Stack:** TypeScript, Node crypto/Buffer, PostgreSQL 18, node:test.

**Spec:** ../specs/2026-09-14-import-provenance-design.md plus the user's V0.7 instructions (authoritative where more precise).

## Global constraints

- Work on feature/import-provenance based on 14e088864b3f78af3914e084652ab14dce5accb5; do not merge or modify main.
- Preserve existing statuses, importId identity, classifications, callback error identity and persistence error authority.
- Source kinds: CSV_TEXT and LOCAL_FILE. Legacy provenance is NULL. No checksum indexes, uniqueness, lookup, deduplication, source tables or adapter hierarchy.
- Store basename only for files; source_path is NULL by default.
- Store byte metadata before strict UTF-8 decode; unexpected filesystem metadata persistence failures use IMPORT_PROVENANCE_ERROR and rethrow the original error after best-effort recovery.
- Represent byte sizes as safe JavaScript numbers; constrain SQL bigint to 0..9007199254740991, accepting NULL.

## Baseline (completed before implementation)

- [x] Fetch repository, confirm clean branch and exact merge-base. Feature branch has only its approved design beyond V0.6.
- [x] npm run verify: typecheck, build, 25 unit and 45 integration tests pass.
- [x] node --test tests/postgres/*.test.mjs against temporary PostgreSQL 18: 11 pass.

## Task 1: Shared byte metadata

Files: src/ingestion/source-provenance.ts; tests/unit/source-provenance.test.mjs.
Interface: sourceContentMetadata(bytes: Uint8Array): { sourceSizeBytes: number; sourceSha256: string }.

- [x] Write tests for empty bytes, abc's known SHA-256, multibyte UTF-8 and a Uint8Array subview. Assert literal known digests and byte lengths.
- [x] Run node --test tests/unit/source-provenance.test.mjs; verify missing helper RED.
- [x] Implement using bytes.byteLength and createHash('sha256').update(bytes).digest('hex').
- [x] Build and run tests; verify GREEN, then commit.

## Task 2: Storage and queries

Files: db/migrations/0002_add_import_provenance.sql; src/db/imports.ts; tests/integration/import-queries.test.mjs; tests/postgres/import-provenance.test.mjs; existing live migration expectations.
Interfaces: ImportSourceKind; nullable ImportSourceMetadata on ImportBatch and ImportSummary; createImportBatch accepts optional metadata; updateImportSourceContentMetadata(db, {importId, sourceSizeBytes, sourceSha256}): Promise<void>.

- [x] Write live tests applying V0.6 migrations, insert legacy rows, apply new migration and check NULL provenance plus idempotency. Test invalid kind, negative/unsafe size and non-lowercase/non-64-hex hash rejection, with zero and safe maximum accepted.
- [x] Write batch/summary mapping tests for NULL and bigint string sizes; insertion and update tests preserve identity fields and reject absent/non-RECEIVED batches without changing state.
- [x] Run new tests and observe missing columns/metadata/operation RED.
- [x] Add nullable columns with CHECK constraints; extend types, INSERT RETURNING and SELECT projections; map bigint strings to numbers. Update only source_size_bytes/source_sha256 using WHERE import_id=$1 AND status='RECEIVED', require rowCount=1.
- [x] Update old fake rows and exact expected objects only to include new nullable fields; retain all original assertions. Update live migration ledger expectations to include 0002.
- [x] Run query, migration and live tests; verify GREEN, then commit.

## Task 3: CSV text provenance

Files: src/ingestion/run-record-import.ts; tests/integration/run-record-import.test.mjs; tests/postgres/import-provenance.test.mjs.

- [x] Write tests for complete metadata before callbacks, duplicate contents under separate IDs, parse/header/row failures and callback/persistence failures retaining provenance. Extend test database metadata storage without changing lifecycle logic.
- [x] Run focused tests and verify missing CSV provenance RED.
- [x] Encode Buffer.from(csvText, 'utf8'), use shared helper and create the batch with CSV_TEXT and complete content metadata. Keep source name/path NULL and post-batch pipeline unchanged.
- [x] Run focused tests and regression suite; verify GREEN, then commit.

## Task 4: Filesystem provenance

Files: src/ingestion/run-record-file-import.ts; tests/integration/run-record-file-import.test.mjs; tests/postgres/import-provenance.test.mjs.

- [x] Write tests for partial provenance at creation/read failure, basename privacy, original bytes (including BOM and malformed UTF-8), metadata timing before decoding/staging, duplicate contents, downstream failures, and provenance database failure with unsuccessful recovery retaining original exception.
- [x] Run focused tests and verify missing LOCAL_FILE metadata RED.
- [x] Create batch with LOCAL_FILE and basename before read; separate read/decode catches; persist helper metadata in between. On update failure, best-effort fail with IMPORT_PROVENANCE_ERROR then throw original exception.
- [x] Run all focused and regression/live tests; verify GREEN, then commit.

## Task 5: Release verification and documentation

Files: README.md; VERIFICATION.txt; this plan.

- [x] Review diff against every requirement; fix any gaps with fresh failing tests before code changes.
- [x] Run npm run verify, npm run build and npm run test:postgres (Git Bash with temporary PostgreSQL configured).
- [x] Document provenance, constraints, lifecycle timing, environment and test results; commit on feature branch.
- [x] Verify clean worktree and report exact HEAD and deviations. No merge to main.

## Execution notes

- Executed inline in a fresh task-specific clone on the requested branch; no existing user checkout was modified.
- RED/GREEN checkpoints: helper missing module; storage live tests missing migration/columns/operation; CSV tests missing metadata; filesystem tests missing metadata and metadata-error handling. All became GREEN after their respective minimal implementations.
- Independent code review reported no actionable findings.
- Local npm run verify: 27 unit and 67 integration tests; typecheck and build pass.
- Local npm run test:postgres: 18 tests pass against PostgreSQL 18.4 with the repository gate script unchanged.
- Final branch HEAD and any remote CI results are reported separately to avoid a self-referential commit hash. No merge or post-merge main gate is performed, per the user instruction.
