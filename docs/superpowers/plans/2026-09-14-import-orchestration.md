# V0.5 End-to-End Import Orchestration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add one supported `runRecordImport(...)` operation that creates an import batch, parses CSV text, prepares/stages records, persists rows/issues, terminalizes pre-staging failures, and returns the committed import summary.

**Architecture:** Keep the existing CSV parser, staging preparation, persistence transaction, and query APIs intact. Add one thin orchestration module plus one narrow database helper for `RECEIVED -> FAILED` batch terminalization. Use typed/source-based failure classification rather than message substring matching, and preserve existing transaction boundaries.

**Tech Stack:** TypeScript, Node.js ESM, Node built-in test runner, PostgreSQL 18 in GitHub Actions, existing `csv-parse` dependency.

**Spec:** `docs/superpowers/specs/2026-09-14-import-orchestration-design.md`

## Global Constraints

- Remain domain-neutral; add no political, voter-specific, or other application-specific semantics.
- Every non-duplicate import attempt creates `import_batch` before parsing.
- Batch-level failures reuse `import_issue` with `row_number = NULL`, `record_id = NULL`, `severity = 'ERROR'`.
- Pre-staging failure transition is exactly `RECEIVED -> FAILED`.
- Ordinary CSV/schema/row-quality failures return a normal terminal result; duplicate IDs, callback exceptions, and infrastructure failures throw.
- Callback failures persist no partial stage rows.
- Database recovery is best-effort and must never replace the original thrown error.
- Add no new database table or lifecycle status.
- Do not expose a generic arbitrary status setter.
- Preserve existing parser whitespace/raw-value behavior and existing `persistRecordStaging` record-ID persistence behavior.
- Use TDD: each behavior begins with a failing test and ends with a green focused test before broader verification.

---

## File Structure

**Create:**
- `src/ingestion/run-record-import.ts` — public orchestration API and branch logic.
- `tests/integration/run-record-import.test.mjs` — end-to-end in-memory orchestration tests.

**Modify:**
- `src/db/imports.ts` — add narrow `failImportBatch(...)` helper.
- `src/ingestion/prepare-record-staging.ts` — introduce a typed callback-failure wrapper that preserves original exception identity.
- `tests/unit/prepare-record-staging.test.mjs` — typed callback failure coverage.
- `tests/integration/import-queries.test.mjs` — transactional `failImportBatch(...)` coverage.
- `tests/postgres/live-postgres.test.mjs` — live PostgreSQL orchestration gate.
- `README.md` — document the supported V0.5 application-level API.
- `VERIFICATION.txt` — record final V0.5 verification evidence only after fresh green runs.

No migration files are expected.

---

### Task 1: Add narrow batch failure terminalization

**Files:**
- Modify: `src/db/imports.ts`
- Test: `tests/integration/import-queries.test.mjs`

**Interfaces:**
- Consumes: existing `Queryable` and `import_batch` / `import_issue` tables.
- Produces:

```ts
export async function failImportBatch(
  db: Queryable,
  input: {
    importId: string;
    issueCode: string;
    detail: string;
  },
): Promise<void>;
```

The helper may only terminalize a batch whose durable state is `RECEIVED`.

- [ ] **Step 1: Write failing integration tests**

Add tests that assert:

```js
await failImportBatch(db, {
  importId: "import-1",
  issueCode: "CSV_PARSE_ERROR",
  detail: "bad csv",
});

assert.equal((await getImportBatch(db, "import-1")).status, "FAILED");
assert.deepEqual(await listImportIssues(db, "import-1"), [
  {
    issueId: 1,
    importId: "import-1",
    rowNumber: null,
    recordId: null,
    issueCode: "CSV_PARSE_ERROR",
    severity: "ERROR",
    fieldKey: null,
    detail: "bad csv",
  },
]);
```

Also assert that calling the helper for a non-`RECEIVED` batch throws and does not insert an issue, and that a simulated insert/update failure rolls back both changes.

- [ ] **Step 2: Run the focused test and confirm RED**

Run:

```bash
npm run build && node --test tests/integration/import-queries.test.mjs
```

Expected: FAIL because `failImportBatch` does not yet exist.

- [ ] **Step 3: Implement the minimal helper**

Use one transaction:

```ts
await db.query("begin");
try {
  const transition = await db.query(
    `update import_batch
     set status = 'FAILED'
     where import_id = $1 and status = 'RECEIVED'
     returning import_id`,
    [input.importId],
  );
  if (transition.rowCount !== 1) {
    throw new Error("Import must be in RECEIVED status before failure terminalization.");
  }
  await db.query(
    `insert into import_issue
       (import_id, row_number, record_id, issue_code, severity, field_key, detail)
     values ($1, null, null, $2, 'ERROR', null, $3)`,
    [input.importId, input.issueCode, input.detail],
  );
  await db.query("commit");
} catch (error) {
  await db.query("rollback");
  throw error;
}
```

- [ ] **Step 4: Re-run focused integration tests**

Expected: PASS.

- [ ] **Step 5: Run the existing import query and persistence regressions**

```bash
npm run build && node --test tests/integration/import-queries.test.mjs tests/integration/persist-record-staging.test.mjs
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/db/imports.ts tests/integration/import-queries.test.mjs
git commit -m "feat: add import failure terminalization"
```

---

### Task 2: Make staging callback failures classifiable without string parsing

**Files:**
- Modify: `src/ingestion/prepare-record-staging.ts`
- Test: `tests/unit/prepare-record-staging.test.mjs`

**Interfaces:**
- Consumes: `transform`, `getRecordId`, and `diagnose` callbacks.
- Produces a typed wrapper that preserves the original thrown value:

```ts
export class RecordStagingCallbackError extends Error {
  readonly rowNumber: number;
  readonly cause: unknown;
}
```

`UnsupportedRecordSchemaError` remains the header/schema discriminator.

- [ ] **Step 1: Write a failing callback-classification test**

In `tests/unit/prepare-record-staging.test.mjs`, add a test where `transform` throws `originalError` and assert:

```js
assert.throws(
  () => prepareRecordStaging(input),
  (error) =>
    error.name === "RecordStagingCallbackError" &&
    error.rowNumber === 1 &&
    error.cause === originalError,
);
```

Also assert the wrapper message still contains `Record staging failed at row 1:`. The same wrapper path covers exceptions from `transform`, `getRecordId`, and `diagnose` because all three execute inside the existing row-level `try` block.

- [ ] **Step 2: Run the focused staging test and confirm RED**

```bash
npm run build && node --test tests/unit/prepare-record-staging.test.mjs
```

Expected: FAIL because `RecordStagingCallbackError` does not yet exist.

- [ ] **Step 3: Implement the typed wrapper**

Add `RecordStagingCallbackError` in `src/ingestion/prepare-record-staging.ts` and replace only the current generic callback rethrow. Set `name = "RecordStagingCallbackError"`, preserve `rowNumber`, preserve the original value as `cause`, and keep the existing row-number message shape. Do not alter header validation, record-ID validation, row diagnostics, or preservation behavior.

- [ ] **Step 4: Re-run the focused staging test**

```bash
npm run build && node --test tests/unit/prepare-record-staging.test.mjs
```

Expected: PASS.

- [ ] **Step 5: Run all staging regressions**

```bash
npm run build && node --test tests/unit/*.test.mjs tests/integration/stage-record-import.test.mjs tests/integration/csv-staging.test.mjs
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/ingestion/prepare-record-staging.ts tests/unit/prepare-record-staging.test.mjs
git commit -m "refactor: classify staging callback failures"
```

---

### Task 3: Add the successful and ordinary-failure orchestration path

**Files:**
- Create: `src/ingestion/run-record-import.ts`
- Create: `tests/integration/run-record-import.test.mjs`

**Interfaces:**
- Consumes:
  - `createImportBatch(db, { importId, schemaVersion })`
  - `parseCsvRecords(csvText)`
  - `prepareRecordStaging(...)`
  - `persistRecordStaging(db, { importId, rows })`
  - `failImportBatch(...)`
  - `getImportSummary(db, importId)`
  - `UnsupportedRecordSchemaError`
- Produces:

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

export type RunRecordImportResult = {
  importId: string;
  status: "VALIDATED" | "FAILED";
  summary: ImportSummary;
};

export async function runRecordImport(
  input: RunRecordImportInput,
): Promise<RunRecordImportResult>;
```

- [ ] **Step 1: Write the happy-path failing test**

Use CSV:

```text
id,name
1,Alice
2,Bob
```

Assert one call returns persisted state equivalent to:

```js
{
  importId: "import-ok",
  status: "VALIDATED",
  summary: {
    importId: "import-ok",
    schemaVersion: "v1",
    status: "VALIDATED",
    rowCount: 2,
    validRowCount: 2,
    invalidRowCount: 0,
    pendingRowCount: 0,
    errorCount: 0,
    warningCount: 0,
  },
}
```

- [ ] **Step 2: Run the focused test and confirm RED**

```bash
npm run build && node --test tests/integration/run-record-import.test.mjs
```

Expected: FAIL because the orchestration module does not exist.

- [ ] **Step 3: Implement the minimal happy path**

The sequence must be exactly:

```ts
await createImportBatch(db, {
  importId: input.importId,
  schemaVersion: input.contract.schemaVersion,
});
const parsed = parseCsvRecords(input.csvText);
const prepared = prepareRecordStaging({
  contract: input.contract,
  headers: parsed.headers,
  rows: parsed.rows,
  transform: input.transform,
  getRecordId: input.getRecordId,
  diagnose: input.diagnose,
});
const persisted = await persistRecordStaging(db, {
  importId: input.importId,
  rows: prepared.rows,
});
const summary = await getImportSummary(db, input.importId);
```

Throw `new Error(`Import summary missing after terminalization: ${input.importId}`)` if `summary === null`. Return `status: persisted.status` and the database summary.

- [ ] **Step 4: Verify happy path GREEN**

Expected: PASS.

- [ ] **Step 5: Add failing tests for ordinary terminal failures**

Add tests for:
- malformed CSV -> durable `CSV_PARSE_ERROR`, status `FAILED`, no stage rows;
- missing/unknown headers -> durable `SCHEMA_HEADER_ERROR`, status `FAILED`, no stage rows;
- row-level `ERROR` diagnostic -> rows/issues persist through `persistRecordStaging`, returns `FAILED` without throwing;
- warnings only -> returns `VALIDATED`.

- [ ] **Step 6: Implement ordinary failure branches**

Catch parser errors around `parseCsvRecords` only, then:

```ts
await failImportBatch(db, {
  importId: input.importId,
  issueCode: "CSV_PARSE_ERROR",
  detail: error instanceof Error ? error.message : String(error),
});
```

Catch `UnsupportedRecordSchemaError` around staging only and terminalize with `SCHEMA_HEADER_ERROR`. Do not catch `RecordStagingCallbackError` as an ordinary result.

After either pre-staging terminalization, call `getImportSummary`. If it is null, throw the same invariant error. Return `{ importId: input.importId, status: "FAILED", summary }`.

- [ ] **Step 7: Run focused orchestration tests**

```bash
npm run build && node --test tests/integration/run-record-import.test.mjs
```

Expected: all ordinary success/failure cases PASS.

- [ ] **Step 8: Commit**

```bash
git add src/ingestion/run-record-import.ts tests/integration/run-record-import.test.mjs
git commit -m "feat: add record import orchestration"
```

---

### Task 4: Add exceptional failure and best-effort recovery semantics

**Files:**
- Modify: `src/ingestion/run-record-import.ts`
- Modify: `tests/integration/run-record-import.test.mjs`

**Interfaces:**
- Consumes: `RecordStagingCallbackError`, `failImportBatch(...)`, persistence/query errors.
- Produces: original-error-preserving exceptional behavior.

- [ ] **Step 1: Write failing callback-exception tests**

Use a callback that throws a stable error instance:

```js
const original = new Error("transform exploded");
```

Assert:
- `runRecordImport(...)` rejects with the exact `original` instance;
- one batch-level `STAGING_CALLBACK_ERROR` exists when recovery succeeds;
- batch is `FAILED`;
- zero stage rows exist.

- [ ] **Step 2: Run focused tests and confirm RED**

```bash
npm run build && node --test tests/integration/run-record-import.test.mjs
```

Expected: FAIL until exceptional callback handling exists.

- [ ] **Step 3: Implement callback failure recovery**

When catching `RecordStagingCallbackError`:

```ts
try {
  await failImportBatch(db, {
    importId: input.importId,
    issueCode: "STAGING_CALLBACK_ERROR",
    detail: error.message,
  });
} catch {
  // best effort only
}
throw error.cause;
```

Do not persist prepared rows.

- [ ] **Step 4: Verify callback tests GREEN**

Expected: PASS.

- [ ] **Step 5: Add failing persistence-failure recovery tests**

Simulate `persistRecordStaging` failure after its transaction rolls back. Assert:
- orchestrator best-effort terminalizes `RECEIVED -> FAILED` with `IMPORT_PERSISTENCE_ERROR`;
- orchestrator rethrows the exact original persistence error.

Add a second case where failure terminalization also fails. Assert the original persistence error still wins.

- [ ] **Step 6: Implement persistence-failure recovery**

Wrap only the persistence call:

```ts
try {
  await persistRecordStaging(db, {
    importId: input.importId,
    rows: prepared.rows,
  });
} catch (error) {
  try {
    await failImportBatch(db, {
      importId: input.importId,
      issueCode: "IMPORT_PERSISTENCE_ERROR",
      detail: error instanceof Error ? error.message : String(error),
    });
  } catch {
    // best effort only
  }
  throw error;
}
```

Do not use this branch for duplicate-ID creation failure, because no new batch exists.

- [ ] **Step 7: Add duplicate-ID and missing-summary tests**

Assert:
- duplicate import ID throws existing `Import batch already exists: <id>` behavior;
- unexpected `null` summary throws `Import summary missing after terminalization: <id>` rather than fabricating a result.

- [ ] **Step 8: Run orchestration integration suite**

```bash
npm run build && node --test tests/integration/run-record-import.test.mjs
```

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add src/ingestion/run-record-import.ts tests/integration/run-record-import.test.mjs
git commit -m "feat: add import orchestration recovery semantics"
```

---

### Task 5: Add live PostgreSQL end-to-end coverage

**Files:**
- Modify: `tests/postgres/live-postgres.test.mjs`

**Interfaces:**
- Consumes: public `runRecordImport(...)` API against migrated PostgreSQL schema.
- Produces: authoritative live-database evidence for V0.5.

- [ ] **Step 1: Add three live PostgreSQL tests**

Cover:
1. successful orchestrated two-row import -> `VALIDATED`, two valid rows;
2. malformed CSV -> durable `CSV_PARSE_ERROR` with null row/record identifiers and `FAILED` status;
3. row-level validation error -> staged row marked `INVALID`, issue persisted, batch `FAILED`.

Use unique import IDs per test and the existing cleanup conventions in `tests/postgres/live-postgres.test.mjs`.

- [ ] **Step 2: Run dependency-complete in-memory verification**

```bash
npm run verify
```

Expected: PASS. This step does not substitute for the PostgreSQL live gate.

- [ ] **Step 3: Commit**

```bash
git add tests/postgres/live-postgres.test.mjs
git commit -m "test: cover orchestrated imports in postgres"
```

---

### Task 6: Document API and run full verification

**Files:**
- Modify: `README.md`
- Modify after successful verification: `VERIFICATION.txt`

**Interfaces:**
- Documents: `runRecordImport(...)`, normal-vs-exception failure semantics, batch-level issues, and V0.5 scope exclusions.

- [ ] **Step 1: Update README**

Add a concise example:

```ts
const result = await runRecordImport({
  db,
  importId: "customer-import-2026-09-14",
  contract,
  csvText,
  transform,
  getRecordId,
  diagnose,
});
```

Document that CSV/schema/row validation failures can return `status: "FAILED"`, while duplicate IDs, callback exceptions, and infrastructure failures throw.

- [ ] **Step 2: Run full dependency-complete verification**

```bash
npm ci
npm run verify
npm run test:postgres
```

Expected:
- typecheck PASS;
- unit suite PASS;
- all in-memory integration tests PASS;
- PostgreSQL 18 live suite PASS.

If the local environment lacks PostgreSQL, run `npm ci && npm run verify` locally and use the existing GitHub Actions PostgreSQL 18 workflow as the authoritative live-database gate. Record its exact run ID and commit SHA.

- [ ] **Step 3: Update `VERIFICATION.txt` only from fresh evidence**

Record:
- exact feature-head SHA;
- unit/integration counts;
- live PostgreSQL test count;
- GitHub Actions run ID;
- confirmation that the live orchestration cases passed.

Do not copy forward a previous run as V0.5 evidence.

- [ ] **Step 4: Commit docs/evidence**

```bash
git add README.md VERIFICATION.txt
git commit -m "docs: document import orchestration verification"
```

- [ ] **Step 5: Perform final regression verification on the exact final feature head**

Run the complete CI gate again after the documentation/evidence commit because that commit changes the feature-head SHA. The final evidence must correspond to the exact head proposed for pull request review.

---

## Final Review Checklist

Before opening or merging a pull request:

- [ ] `runRecordImport(...)` is the only new high-level orchestration entry point.
- [ ] Existing parser/staging/persistence/query APIs still work independently.
- [ ] `failImportBatch(...)` cannot mutate non-`RECEIVED` batches.
- [ ] No migration/table/status was added.
- [ ] No error classification depends on substring matching human-readable messages.
- [ ] Ordinary bad-input paths return persisted `FAILED` summaries.
- [ ] Callback exceptions rethrow the original callback error and persist no rows.
- [ ] Persistence recovery is best-effort and original-error preserving.
- [ ] Duplicate IDs remain exceptions.
- [ ] No hidden trimming/coercion or record-ID behavior changes were introduced.
- [ ] Exact final-head GitHub Actions verification is green before merge.
