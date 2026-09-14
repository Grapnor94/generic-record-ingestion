# Import Lifecycle and Query API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a domain-neutral database API for creating import batches and reading batches, staged rows, validation issues, and aggregate import summaries without duplicating lifecycle mutation logic.

**Architecture:** Add one focused `src/db/imports.ts` module that accepts the existing `Queryable` interface from staging persistence and maps PostgreSQL snake_case rows to stable camelCase public objects. Existing validation persistence remains the only owner of `RECEIVED -> VALIDATING -> VALIDATED | FAILED` transitions; this phase adds creation and read/query operations only.

**Tech Stack:** TypeScript, Node.js 22, PostgreSQL 18, `pg` 8, Node's built-in test runner.

**Spec:** `docs/superpowers/specs/2026-09-14-import-lifecycle-query-api-design.md`

## Global Constraints

- The API must remain framework-neutral and accept the existing `Queryable` interface.
- Do not expose an arbitrary import-status setter.
- Do not add a database migration or speculative index in this phase.
- Keep database columns snake_case and public TypeScript objects camelCase.
- Translate only PostgreSQL unique violation `23505` during batch creation into `Import batch already exists: <importId>`; propagate all other database errors unchanged.
- Missing batch/summary returns `null`; missing row/issue matches return `[]`.
- Row lists order by `row_number ASC`.
- Issue lists order by `row_number ASC NULLS FIRST, issue_id ASC`.
- Summary aggregation must avoid row/issue join multiplication.
- Keep bigint test values within JavaScript's safe integer range.

---

### Task 1: Import batch creation and lookup

**Files:**
- Create: `src/db/imports.ts`
- Create: `tests/integration/import-queries.test.mjs`

**Interfaces:**
- Consumes: `Queryable` from `src/ingestion/persist-record-staging.ts`
- Produces:
  - `ImportBatchStatus`
  - `ImportBatch`
  - `createImportBatch(db, input): Promise<ImportBatch>`
  - `getImportBatch(db, importId): Promise<ImportBatch | null>`

- [ ] **Step 1: Write failing tests for successful creation, duplicate translation, non-duplicate propagation, retrieval, and missing retrieval**

Use a focused fake `Queryable` that records SQL and parameters and returns caller-supplied results/errors. Assert:
- insert uses the supplied `importId` and `schemaVersion` unchanged;
- returned snake_case row maps to camelCase `ImportBatch`;
- error with `{ code: "23505" }` becomes `Import batch already exists: IMP-1`;
- another error object is rethrown by identity;
- lookup returns a mapped batch or `null`.

- [ ] **Step 2: Run the focused test and verify RED**

Run:
```bash
npm run build && node --test tests/integration/import-queries.test.mjs
```
Expected: FAIL because `dist/db/imports.js` does not exist or required exports are missing.

- [ ] **Step 3: Implement batch types and mapping helpers**

In `src/db/imports.ts`, define the public unions/types from the approved spec plus an internal snake_case database-row type and a `mapImportBatch` helper.

- [ ] **Step 4: Implement `createImportBatch` minimally**

Use one `insert ... returning import_id, schema_version, status, created_at, updated_at` query. Catch only errors whose object-like value has `code === "23505"`; throw `new Error(`Import batch already exists: ${input.importId}`)` for that case and rethrow everything else unchanged.

- [ ] **Step 5: Implement `getImportBatch` minimally**

Use one select filtered by `import_id = $1`; map `rows[0]` or return `null`.

- [ ] **Step 6: Run focused and regression tests**

Run:
```bash
npm run typecheck
npm run build && node --test tests/integration/import-queries.test.mjs
npm run test:integration
```
Expected: all PASS.

- [ ] **Step 7: Commit**

```bash
git add src/db/imports.ts tests/integration/import-queries.test.mjs
git commit -m "feat: add import batch creation and lookup"
```

---

### Task 2: Staged row and issue queries

**Files:**
- Modify: `src/db/imports.ts`
- Modify: `tests/integration/import-queries.test.mjs`

**Interfaces:**
- Consumes: Task 1 module and `Queryable`
- Produces:
  - `ImportRowStatus`
  - `ImportRow`
  - `ImportIssueSeverity`
  - `ImportIssue`
  - `listImportRows(db, importId, options?): Promise<ImportRow[]>`
  - `listImportIssues(db, importId, options?): Promise<ImportIssue[]>`

- [ ] **Step 1: Add failing row-query tests**

Assert the no-filter query:
- uses only `$1 = importId`;
- orders by `row_number ASC`;
- maps JSONB and nullable `raw_source_row` correctly.

Assert filtered query:
- adds `validation_status = $2`;
- passes `[importId, "INVALID"]` exactly.

- [ ] **Step 2: Add failing issue-query tests**

Cover four query shapes:
- no filters;
- severity only;
- row number only;
- both filters.

Assert deterministic `ORDER BY row_number ASC NULLS FIRST, issue_id ASC` and stable parameter order: import ID first, severity second when present, row number after severity when both are present.

- [ ] **Step 3: Run focused tests and verify RED**

Run:
```bash
npm run build && node --test tests/integration/import-queries.test.mjs
```
Expected: FAIL because row/issue exports are missing.

- [ ] **Step 4: Implement row types, mapper, and `listImportRows`**

Select:
`import_id, row_number, record_id, source_row, raw_source_row, validation_status`
from `import_stage_row`, filter by import ID and optional status, and order by row number ascending.

- [ ] **Step 5: Implement issue types, mapper, and `listImportIssues`**

Select:
`issue_id, import_id, row_number, record_id, issue_code, severity, field_key, detail`
from `import_issue`; construct predicates and parameters deterministically without exposing arbitrary SQL.

- [ ] **Step 6: Run focused and regression tests**

Run:
```bash
npm run typecheck
npm run build && node --test tests/integration/import-queries.test.mjs
npm run test:integration
```
Expected: all PASS.

- [ ] **Step 7: Commit**

```bash
git add src/db/imports.ts tests/integration/import-queries.test.mjs
git commit -m "feat: add import row and issue queries"
```

---

### Task 3: Aggregate import summary

**Files:**
- Modify: `src/db/imports.ts`
- Modify: `tests/integration/import-queries.test.mjs`

**Interfaces:**
- Consumes: Task 1 batch types and existing tables
- Produces:
  - `ImportSummary`
  - `getImportSummary(db, importId): Promise<ImportSummary | null>`

- [ ] **Step 1: Add failing summary tests**

Cover:
- populated summary with mixed row statuses and issue severities;
- existing batch with zero rows/issues returning zeroes;
- missing batch returning `null`.

Assert the SQL contains independent aggregation for staged rows and issues rather than a direct row-to-issue multiplication join.

- [ ] **Step 2: Run focused tests and verify RED**

Run:
```bash
npm run build && node --test tests/integration/import-queries.test.mjs
```
Expected: FAIL because `getImportSummary` is missing.

- [ ] **Step 3: Implement `ImportSummary` and summary mapping**

Return camelCase fields:
`importId`, `schemaVersion`, `status`, `rowCount`, `validRowCount`, `invalidRowCount`, `pendingRowCount`, `errorCount`, `warningCount`.

- [ ] **Step 4: Implement `getImportSummary` with independent aggregates**

Use `import_batch` as the driving table and independent lateral/subquery aggregates for `import_stage_row` and `import_issue`, each filtered by the same import ID. Ensure `COALESCE(..., 0)` yields zeros and no batch row yields `null`.

- [ ] **Step 5: Run focused and regression tests**

Run:
```bash
npm run typecheck
npm run build && node --test tests/integration/import-queries.test.mjs
npm run verify
```
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add src/db/imports.ts tests/integration/import-queries.test.mjs
git commit -m "feat: add import summary query"
```

---

### Task 4: Live PostgreSQL acceptance coverage

**Files:**
- Modify: `tests/postgres/live-postgres.test.mjs`

**Interfaces:**
- Consumes: all five Task 1-3 public functions plus `persistRecordStaging`
- Produces: authoritative live-database behavior proof for the API

- [ ] **Step 1: Add a live lifecycle/query test after clean-schema migration bootstrap**

In an isolated schema:
1. call `createImportBatch` and assert `RECEIVED`;
2. call `getImportBatch` and assert persisted metadata;
3. call duplicate `createImportBatch` and assert exact stable message;
4. create prepared rows with one valid row, one warning-only row, and one ERROR row as needed to exercise query counts;
5. call `persistRecordStaging`;
6. assert `listImportRows` ordering and status filtering;
7. assert `listImportIssues` ordering plus severity/row filters;
8. assert `getImportSummary` counts and final status;
9. assert missing-import batch/summary/rows/issues behavior.

- [ ] **Step 2: Run syntax and local non-live verification**

Run:
```bash
node --check tests/postgres/live-postgres.test.mjs
npm run verify
```
Expected: PASS.

- [ ] **Step 3: Run live PostgreSQL locally only if PostgreSQL tooling is available**

Run:
```bash
npm run test:postgres
```
Expected: PASS when `psql`/PostgreSQL are available; otherwise record the prerequisite limitation without marking live verification PASS.

- [ ] **Step 4: Commit**

```bash
git add tests/postgres/live-postgres.test.mjs
git commit -m "test: verify import query API on PostgreSQL"
```

---

### Task 5: Documentation and release verification record

**Files:**
- Modify: `README.md`
- Modify: `VERIFICATION.txt`

**Interfaces:**
- Consumes: implemented public API and observed verification results
- Produces: operator/developer documentation and truthful test record

- [ ] **Step 1: Document the import lifecycle/query API in README**

Add a concise section listing the five public functions and state explicitly that lifecycle transitions are still owned by `persistRecordStaging`; there is no arbitrary status setter.

- [ ] **Step 2: Update `VERIFICATION.txt` with only locally observed results**

Record actual typecheck/unit/integration results. Mark the new live GitHub Actions validation as pending until a branch workflow succeeds.

- [ ] **Step 3: Run fresh final local verification**

Run:
```bash
npm run verify
node --check tests/postgres/live-postgres.test.mjs
bash -n scripts/run-postgres-integration.sh
git diff --check
```
Expected: all locally executable checks PASS.

- [ ] **Step 4: Commit**

```bash
git add README.md VERIFICATION.txt
git commit -m "docs: document import lifecycle query API"
```

---

### Task 6: Authoritative GitHub Actions gate and PR integration

**Files:**
- Modify `VERIFICATION.txt` only after observed CI success if needed.

**Interfaces:**
- Consumes: feature branch produced by Tasks 1-5
- Produces: authoritative PostgreSQL 18 CI evidence and merge-ready branch

- [ ] **Step 1: Push feature branch**

Push `feature/import-lifecycle-query-api` to GitHub without force-pushing.

- [ ] **Step 2: Verify the push-triggered PostgreSQL integration workflow**

Require the exact feature head SHA to complete with:
- dependency-free verification PASS;
- live PostgreSQL gate PASS.

If it fails, inspect the failing job logs before changing code.

- [ ] **Step 3: Record authoritative CI evidence**

After observed success, update `VERIFICATION.txt` with the exact run/result and commit that documentation-only change. Re-run/observe CI for the verification commit if the workflow triggers on push.

- [ ] **Step 4: Open a pull request into `main`**

Summarize the API, lifecycle ownership, test coverage, and CI evidence.

- [ ] **Step 5: Verify PR-triggered checks**

Require the PR head workflow to pass before merge.

- [ ] **Step 6: Merge only after green checks**

Prefer squash merge to keep `main` history compact, then confirm the resulting `main` commit SHA.
