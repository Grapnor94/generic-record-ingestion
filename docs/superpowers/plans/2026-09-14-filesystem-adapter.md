# Filesystem Adapter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `runRecordFileImport(...)` so callers can import a local CSV file by path while preserving V0.5 orchestration semantics and durably recording file-read failures.

**Architecture:** Keep `runRecordImport(...)` public behavior unchanged, but extract its post-batch CSV pipeline into an internal helper. Add a filesystem wrapper that creates the batch first, reads bytes, performs strict/fatal UTF-8 decoding, records `FILE_READ_ERROR` for expected read/decode failures, and otherwise delegates to the shared pipeline.

**Tech Stack:** TypeScript 5.8, Node.js 22 APIs (`node:fs/promises`, `TextDecoder`), Node test runner, PostgreSQL 18 CI gate.

**Spec:** `docs/superpowers/specs/2026-09-14-filesystem-adapter-design.md`

## Global Constraints

- `runRecordImport(...)` remains source-compatible and continues to accept `csvText`.
- Every non-duplicate filesystem import creates the durable batch before attempting file access.
- Decode CSV files as strict UTF-8; malformed byte sequences are `FILE_READ_ERROR`.
- File-read/decode failures return a committed `FAILED` result; they do not throw.
- After successful file decoding, all V0.5 CSV/header/staging/persistence return-vs-throw semantics remain unchanged.
- No new database status, table, migration, external dependency, stream API, upload API, alternate encoding, delimiter detection, retry, or directory behavior.
- Use TDD: demonstrate RED before each behavior-changing implementation commit.

---

## File Structure

- Modify `src/ingestion/run-record-import.ts` — expose the existing public text entry point and an internal post-batch CSV pipeline with no batch creation.
- Create `src/ingestion/run-record-file-import.ts` — own file-path access, strict UTF-8 decoding, durable `FILE_READ_ERROR`, and delegation to the internal CSV pipeline.
- Create `tests/integration/run-record-file-import.test.mjs` — filesystem behavior and V0.5 semantic parity tests using temporary files and an in-memory database double.
- Modify `tests/integration/run-record-import.test.mjs` only if a reusable in-memory DB test helper is extracted; otherwise leave it intact as regression coverage.
- Modify `tests/postgres/live-postgres.test.mjs` — add live successful file import, file-read failure, and downstream row-error coverage.
- Modify `README.md` — document `runRecordFileImport(...)`, strict UTF-8, and return-vs-throw behavior.
- Modify `VERIFICATION.txt` — record exact feature-head test and PostgreSQL evidence.

### Task 1: Extract the shared post-batch CSV pipeline without changing V0.5 behavior

**Files:**
- Modify: `src/ingestion/run-record-import.ts`
- Test: `tests/integration/run-record-import.test.mjs`

**Interfaces:**
- Consumes: existing `RunRecordImportInput`, `RunRecordImportResult`, `createImportBatch(...)`, CSV/staging/persistence primitives.
- Produces: internal `runRecordImportAfterBatch(input)` accepting the same import fields except batch creation is already complete; `runRecordImport(...)` remains the public API and calls `createImportBatch(...)` exactly once before the helper.

- [ ] **Step 1: Add a regression test that proves public text imports still create one batch and preserve the existing result**

Add a test around the existing `MemoryImportDb` that counts `insert into import_batch` calls and runs:

```js
const result = await runRecordImport({
  db,
  importId: "text-regression",
  contract: { schemaVersion: "v1", requiredHeaders: ["record_id", "name"] },
  csvText: "record_id,name\nR-1,Ada\n",
  transform: (row) => ({ name: row.name }),
  getRecordId: (row) => row.record_id,
});

assert.equal(result.status, "VALIDATED");
assert.equal(db.batchInsertCount, 1);
assert.equal(result.summary.rowCount, 1);
```

- [ ] **Step 2: Run the focused integration test before refactoring**

Run: `npm run test:integration -- --test-name-pattern="text imports still create one batch"`

Expected: PASS on the existing implementation. This is characterization coverage; do not change behavior to manufacture a failure.

- [ ] **Step 3: Refactor the existing function into public batch creation plus an internal helper**

In `src/ingestion/run-record-import.ts`, define an internal input type that omits `db`/`importId` duplication only if it improves clarity; the required callable shape is:

```ts
export async function runRecordImportAfterBatch(
  input: RunRecordImportInput,
): Promise<RunRecordImportResult> {
  // Existing parse -> prepare -> persist -> summary logic only.
}

export async function runRecordImport(
  input: RunRecordImportInput,
): Promise<RunRecordImportResult> {
  await createImportBatch(input.db, {
    importId: input.importId,
    schemaVersion: input.contract.schemaVersion,
  });
  return runRecordImportAfterBatch(input);
}
```

`runRecordImportAfterBatch(...)` is an internal module-level interface for the filesystem adapter, not a documented consumer API. It must not call `createImportBatch(...)`.

- [ ] **Step 4: Run all existing verification**

Run: `npm run verify`

Expected: typecheck PASS, unit tests PASS, integration tests PASS with no changed V0.5 semantics.

- [ ] **Step 5: Commit the refactor**

```bash
git add src/ingestion/run-record-import.ts tests/integration/run-record-import.test.mjs
git commit -m "refactor: share post-batch import pipeline"
```

### Task 2: Add strict filesystem reading and durable file-read failure handling

**Files:**
- Create: `src/ingestion/run-record-file-import.ts`
- Create: `tests/integration/run-record-file-import.test.mjs`

**Interfaces:**
- Consumes: `Queryable`, `RecordSchemaContract`, `StagingDiagnostic`, `RunRecordImportResult`, `createImportBatch(...)`, `failImportBatch(...)`, `getImportSummary(...)`, `runRecordImportAfterBatch(...)`.
- Produces:

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

export async function runRecordFileImport(
  input: RunRecordFileImportInput,
): Promise<RunRecordImportResult>;
```

- [ ] **Step 1: Write RED integration tests for valid file, missing file, and invalid UTF-8**

Use `mkdtemp`, `writeFile`, `rm`, and `tmpdir` to create isolated fixtures. Required assertions include:

```js
const ok = await runRecordFileImport({
  db,
  importId: "file-ok",
  contract,
  filePath,
  transform: (row) => ({ name: row.name }),
  getRecordId: (row) => row.record_id,
});
assert.equal(ok.status, "VALIDATED");
assert.equal(ok.summary.rowCount, 1);
```

For a missing path:

```js
const failed = await runRecordFileImport({ ...input, importId: "missing", filePath: missingPath });
assert.equal(failed.status, "FAILED");
assert.equal(db.stage.length, 0);
assert.equal(db.issues.length, 1);
assert.equal(db.issues[0].issue_code, "FILE_READ_ERROR");
assert.equal(db.issues[0].row_number, null);
```

For malformed UTF-8, write bytes such as:

```js
await writeFile(filePath, Buffer.from([0x72, 0x65, 0x63, 0x6f, 0x72, 0x64, 0x5f, 0x69, 0x64, 0x0a, 0xc3, 0x28]));
```

and assert `FAILED` + `FILE_READ_ERROR` + no staged rows.

- [ ] **Step 2: Run the new integration test and verify RED**

Run: `npm run build && node --test tests/integration/run-record-file-import.test.mjs`

Expected: FAIL because `dist/ingestion/run-record-file-import.js` does not exist.

- [ ] **Step 3: Implement the minimal filesystem adapter with strict decoding**

Create `src/ingestion/run-record-file-import.ts` using byte reads and fatal decoding:

```ts
import { readFile } from "node:fs/promises";
import { createImportBatch, failImportBatch, getImportSummary } from "../db/imports.js";
import { runRecordImportAfterBatch, type RunRecordImportInput, type RunRecordImportResult } from "./run-record-import.js";

function errorDetail(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

async function failedFileResult(
  input: RunRecordFileImportInput,
  error: unknown,
): Promise<RunRecordImportResult> {
  await failImportBatch(input.db, {
    importId: input.importId,
    issueCode: "FILE_READ_ERROR",
    detail: errorDetail(error),
  });
  const summary = await getImportSummary(input.db, input.importId);
  if (summary === null) {
    throw new Error(`Import summary missing after terminalization: ${input.importId}`);
  }
  return { importId: input.importId, status: "FAILED", summary };
}

export async function runRecordFileImport(
  input: RunRecordFileImportInput,
): Promise<RunRecordImportResult> {
  await createImportBatch(input.db, {
    importId: input.importId,
    schemaVersion: input.contract.schemaVersion,
  });

  let csvText: string;
  try {
    const bytes = await readFile(input.filePath);
    csvText = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch (error) {
    return failedFileResult(input, error);
  }

  return runRecordImportAfterBatch({ ...input, csvText });
}
```

Define `RunRecordFileImportInput` explicitly rather than exposing `csvText`; when delegating, construct only fields accepted by `RunRecordImportInput` so `filePath` does not become part of the CSV API contract.

- [ ] **Step 4: Run focused and full integration verification**

Run:

```bash
npm run build && node --test tests/integration/run-record-file-import.test.mjs
npm run verify
```

Expected: new filesystem tests PASS; all existing unit/integration tests PASS.

- [ ] **Step 5: Commit filesystem behavior**

```bash
git add src/ingestion/run-record-file-import.ts tests/integration/run-record-file-import.test.mjs
git commit -m "feat: add filesystem import adapter"
```

### Task 3: Prove downstream semantic parity through the file entry point

**Files:**
- Modify: `tests/integration/run-record-file-import.test.mjs`

**Interfaces:**
- Consumes: `runRecordFileImport(...)` from Task 2 and the existing in-memory import database behavior.
- Produces: regression evidence that the wrapper does not reinterpret downstream V0.5 outcomes.

- [ ] **Step 1: Add tests for malformed CSV, headers, row errors, warnings, callbacks, persistence, and duplicate IDs**

Add readable temporary-file cases asserting:

```js
// malformed CSV
assert.equal(result.status, "FAILED");
assert.equal(db.issues[0].issue_code, "CSV_PARSE_ERROR");

// invalid headers
assert.equal(result.status, "FAILED");
assert.equal(db.issues[0].issue_code, "SCHEMA_HEADER_ERROR");

// row diagnostic ERROR
assert.equal(result.status, "FAILED");
assert.equal(db.stage[0].validation_status, "INVALID");

// warnings only
assert.equal(result.status, "VALIDATED");
```

For callback identity:

```js
const callbackError = new Error("callback exploded");
await assert.rejects(
  runRecordFileImport({ ...input, transform: () => { throw callbackError; } }),
  (error) => error === callbackError,
);
assert.equal(db.issues.at(-1).issue_code, "STAGING_CALLBACK_ERROR");
```

For persistence identity, configure the existing DB double's stage insert failure and assert the exact same error object is rethrown plus `IMPORT_PERSISTENCE_ERROR`. For duplicate IDs, pre-create/use the same ID twice and assert the existing `Import batch already exists: <id>` error.

- [ ] **Step 2: Run the expanded filesystem integration suite**

Run: `npm run build && node --test tests/integration/run-record-file-import.test.mjs`

Expected: PASS. If a case fails, fix only the adapter/shared-helper boundary; do not change established V0.5 downstream semantics.

- [ ] **Step 3: Run full dependency-free verification**

Run: `npm run verify`

Expected: PASS.

- [ ] **Step 4: Commit semantic-parity coverage**

```bash
git add tests/integration/run-record-file-import.test.mjs src/ingestion/run-record-file-import.ts src/ingestion/run-record-import.ts
git commit -m "test: cover filesystem import failure semantics"
```

### Task 4: Add live PostgreSQL filesystem coverage

**Files:**
- Modify: `tests/postgres/live-postgres.test.mjs`

**Interfaces:**
- Consumes: `runRecordFileImport(...)`, existing migration/database query helpers, Node temporary-file APIs.
- Produces: three real PostgreSQL integration tests for the filesystem entry point.

- [ ] **Step 1: Add RED/live tests for successful file import, missing file, and downstream row error**

Import `runRecordFileImport` and Node temp-file helpers. Within `withDatabase(...)`, run migrations before each import case.

Successful case must assert persisted batch `VALIDATED`, expected raw/canonical row data, and zero errors. Missing-file case must assert batch `FAILED`, no rows, and exactly one batch-level `FILE_READ_ERROR`. Row-error case must assert a persisted `INVALID` row and `FAILED` batch using a caller-supplied `ERROR` diagnostic.

- [ ] **Step 2: Run the PostgreSQL gate and verify the new tests exercise real persistence**

Run: `npm run test:postgres`

Expected: PASS against PostgreSQL. In environments without PostgreSQL/network support, use the repository's GitHub Actions PostgreSQL integration workflow as the authoritative gate and record the exact run ID/SHA.

- [ ] **Step 3: Run full verification again**

Run: `npm run verify`

Expected: PASS.

- [ ] **Step 4: Commit live database coverage**

```bash
git add tests/postgres/live-postgres.test.mjs
git commit -m "test: verify filesystem imports in PostgreSQL"
```

### Task 5: Document and verify V0.6 at the exact feature head

**Files:**
- Modify: `README.md`
- Modify: `VERIFICATION.txt`

**Interfaces:**
- Consumes: completed V0.6 API and exact verification results.
- Produces: user-facing usage documentation and auditable release evidence.

- [ ] **Step 1: Update README with the supported filesystem API**

Add an example equivalent to:

```ts
const result = await runRecordFileImport({
  db,
  importId: "import-2026-09-14",
  contract,
  filePath: "/data/records.csv",
  transform,
  getRecordId,
  diagnose,
});
```

Document that the entire file is read before parsing, strict UTF-8 is required, `FILE_READ_ERROR` is a durable batch-level error, expected read/decode failures return `FAILED`, and downstream behavior matches `runRecordImport(...)`. Explicitly list streams/uploads/alternate encodings as out of scope.

- [ ] **Step 2: Run the exact-head dependency-free verification**

Run: `npm run verify`

Expected: PASS. Record exact unit/integration counts from the output rather than copying historical counts.

- [ ] **Step 3: Run the exact-head live PostgreSQL gate**

Run: `npm run test:postgres` or the GitHub Actions PostgreSQL integration workflow on the exact feature-head SHA.

Expected: PASS. Record the exact live-test count, workflow run ID, and feature-head SHA.

- [ ] **Step 4: Update `VERIFICATION.txt` with only observed evidence**

Record:

```text
V0.6 filesystem adapter
Feature head: <exact SHA>
Dependency-free verification: PASS (<observed counts>)
Live PostgreSQL gate: PASS (<observed counts>)
Workflow run: <exact run ID if CI is authoritative>
```

Also record semantic checks: strict UTF-8, durable `FILE_READ_ERROR`, no staged rows on read failure, downstream V0.5 parity, and unchanged `runRecordImport(...)` public API.

- [ ] **Step 5: Commit documentation and verification evidence**

```bash
git add README.md VERIFICATION.txt
git commit -m "docs: record filesystem adapter verification"
```

- [ ] **Step 6: Run one final exact-head verification after the documentation commit**

Run the GitHub Actions PostgreSQL integration workflow at the new exact feature head and require all workflow steps to complete successfully before opening/merging the PR. Do not claim V0.6 complete while this run is pending.
