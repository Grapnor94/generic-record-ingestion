# CSV Adapter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a domain-neutral synchronous UTF-8 comma-separated CSV adapter that produces the existing `{ headers, rows }` staging input shape while preserving decoded source strings exactly.

**Architecture:** Add one focused adapter module upstream of `prepareRecordStaging`. Use a mature Node-compatible CSV parser for CSV syntax, wrap parser failures in stable `CSV parse failed:` errors, and keep schema validation, transformation, IDs, diagnostics, lifecycle, and persistence in their existing layers.

**Tech Stack:** TypeScript, Node.js 22 ESM, Node built-in test runner, a mature CSV parsing runtime dependency selected by a focused behavior test, existing PostgreSQL 18 CI regression gate.

**Spec:** `docs/superpowers/specs/2026-09-14-csv-adapter-design.md`

## Global Constraints

- V0.4 accepts already-decoded UTF-8 JavaScript text and comma-separated CSV only.
- The first CSV record is the header row.
- Support RFC-style quoted fields, commas in quotes, escaped double quotes, LF/CRLF, empty fields, embedded quoted newlines, and an initial UTF-8 BOM.
- Preserve decoded header and field strings exactly except removal of a BOM only at the beginning of the input.
- Do not trim, coerce, normalize, rename, deduplicate, or perform domain cleanup.
- Reject empty/no-header input, malformed CSV syntax, and data rows whose field count differs from the header width.
- All public adapter failures use the stable prefix `CSV parse failed: `.
- The adapter must not perform schema-contract validation, record-ID validation, diagnostics, lifecycle mutation, or persistence.
- No database migration, streaming, delimiter detection, TSV/pipe support, spreadsheet support, encoding detection, filesystem API, or HTTP upload API.

---

### Task 1: CSV parser dependency and core adapter

**Files:**
- Create: `src/csv/parse-csv-records.ts`
- Create: `tests/unit/parse-csv-records.test.mjs`
- Modify: `package.json`
- Modify: `package-lock.json`

**Interfaces:**
- Consumes: a JavaScript `string` containing already-decoded UTF-8 CSV text.
- Produces: `ParsedCsvRecords` and `parseCsvRecords(input: string): ParsedCsvRecords`.

- [ ] **Step 1: Add focused failing tests for the public contract**

Create tests that import `parseCsvRecords` from `dist/csv/parse-csv-records.js` and assert exact output for ordinary rows, quoted commas, escaped quotes, LF, CRLF, BOM removal, empty fields, leading/trailing whitespace, and an embedded newline inside a quoted field.

Representative assertions:

```js
assert.deepEqual(parseCsvRecords("id,name\n1,Alice\n2,Bob\n"), {
  headers: ["id", "name"],
  rows: [
    { id: "1", name: "Alice" },
    { id: "2", name: "Bob" },
  ],
});

assert.deepEqual(parseCsvRecords('id,name,note\n1,"Smith, Alice","He said ""hello"""\n'), {
  headers: ["id", "name", "note"],
  rows: [{ id: "1", name: "Smith, Alice", note: 'He said "hello"' }],
});

assert.deepEqual(parseCsvRecords("\uFEFFid,value\r\n1,  keep me  \r\n"), {
  headers: ["id", "value"],
  rows: [{ id: "1", value: "  keep me  " }],
});
```

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```bash
npm run build && node --test tests/unit/parse-csv-records.test.mjs
```

Expected: FAIL because `dist/csv/parse-csv-records.js` does not exist.

- [ ] **Step 3: Select and install the parser dependency**

Evaluate a mature Node 22/ESM-compatible CSV parser against the Task 1 cases. Prefer a parser that can return arrays rather than objects so header preservation and duplicate-header behavior are not silently changed by the dependency. Install the selected package as a runtime dependency so its exact resolved version is recorded in `package-lock.json`.

Run the package's smallest direct probe necessary to confirm quoted commas, escaped quotes, BOM handling strategy, strict row width, and useful parse-location metadata before writing the wrapper.

- [ ] **Step 4: Implement the minimal adapter**

Create:

```ts
export type ParsedCsvRecords = {
  headers: string[];
  rows: Record<string, string>[];
};

export function parseCsvRecords(input: string): ParsedCsvRecords;
```

Implementation requirements:

1. Remove `\uFEFF` only when it is `input[0]`.
2. Parse comma-delimited CSV in strict mode with values returned as strings and records available as arrays.
3. Require at least one parsed record for the header.
4. Preserve the header strings exactly.
5. Require every data record to have exactly `headers.length` fields.
6. Build each row object by assigning `row[headers[columnIndex]] = value` without trimming or coercion.
7. Catch parser syntax errors and throw a new `Error` beginning with `CSV parse failed: ` with deterministic detail and location when available.

- [ ] **Step 5: Run focused tests and verify GREEN**

Run:

```bash
npm run typecheck
npm run build && node --test tests/unit/parse-csv-records.test.mjs
```

Expected: all CSV happy-path tests PASS.

- [ ] **Step 6: Commit the core adapter**

```bash
git add package.json package-lock.json src/csv/parse-csv-records.ts tests/unit/parse-csv-records.test.mjs
git commit -m "feat: add CSV record adapter"
```

---

### Task 2: Deterministic structural failures

**Files:**
- Modify: `src/csv/parse-csv-records.ts`
- Modify: `tests/unit/parse-csv-records.test.mjs`

**Interfaces:**
- Consumes: Task 1 `parseCsvRecords`.
- Produces: stable structural error behavior with prefix `CSV parse failed: `.

- [ ] **Step 1: Add failing tests for structural errors**

Add exact/regex assertions for:

```js
assert.throws(
  () => parseCsvRecords(""),
  /^Error: CSV parse failed: input does not contain a header row\.$/,
);

assert.throws(
  () => parseCsvRecords("id,name\n1\n"),
  /^Error: CSV parse failed: row 2 has 1 fields; expected 2\.$/,
);

assert.throws(
  () => parseCsvRecords("id,name\n1,Alice,extra\n"),
  /^Error: CSV parse failed: row 2 has 3 fields; expected 2\.$/,
);

assert.throws(
  () => parseCsvRecords('id,note\n1,"unterminated'),
  /^Error: CSV parse failed: .*unterminated quoted field.*$/i,
);
```

Here `row 2` means CSV record number including the header as row 1. For multiline quoted records, parser-provided physical line metadata may additionally be included in malformed-syntax messages.

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```bash
npm run build && node --test tests/unit/parse-csv-records.test.mjs
```

Expected: at least one structural-error assertion FAILS until deterministic translation is complete.

- [ ] **Step 3: Implement deterministic error translation**

Ensure empty input/no parsed header yields exactly:

```text
CSV parse failed: input does not contain a header row.
```

Ensure width mismatches yield exactly:

```text
CSV parse failed: row <N> has <actual> fields; expected <expected>.
```

For parser syntax failures, map known unterminated-quote errors to adapter-owned wording containing `unterminated quoted field` and append parser line/record metadata only when reliable. For other parser syntax failures, retain the stable prefix and a concise normalized detail rather than exposing a stack trace or unstable native object serialization.

- [ ] **Step 4: Run focused and full unit tests**

Run:

```bash
npm run typecheck
npm test
```

Expected: CSV tests and the full unit suite PASS.

- [ ] **Step 5: Commit structural error behavior**

```bash
git add src/csv/parse-csv-records.ts tests/unit/parse-csv-records.test.mjs
git commit -m "test: enforce CSV structural errors"
```

---

### Task 3: Adapter-to-staging integration

**Files:**
- Create: `tests/integration/csv-staging.test.mjs`

**Interfaces:**
- Consumes: `parseCsvRecords`, `prepareRecordStaging`, `RecordSchemaContract`, caller `transform`, `getRecordId`, and optional `diagnose`.
- Produces: regression proof that CSV output composes directly with the existing staging boundary without moving responsibilities into the adapter.

- [ ] **Step 1: Write the integration test**

Use CSV containing preserved whitespace and an empty value:

```csv
id,name,note
A-1,"  Alice  ",
A-2,Bob,review
```

Parse it, then call `prepareRecordStaging` with:

```js
const contract = {
  schemaVersion: "csv-test-v1",
  requiredHeaders: ["id", "name", "note"],
};

const prepared = prepareRecordStaging({
  contract,
  headers: parsed.headers,
  rows: parsed.rows,
  transform: row => ({
    id: row.id,
    name: row.name.trim(),
    note: row.note,
  }),
  getRecordId: (_raw, canonical) => canonical.id,
  diagnose: raw => raw.note === "review"
    ? [{ code: "REVIEW", severity: "WARNING", fieldKey: "note", detail: "Needs review" }]
    : [],
});
```

Assert:

```js
assert.equal(prepared.rows[0].rawSourceRow.name, "  Alice  ");
assert.equal(prepared.rows[0].sourceRow.name, "Alice");
assert.equal(prepared.rows[0].rawSourceRow.note, "");
assert.equal(prepared.rows[1].diagnostics[0].code, "REVIEW");
assert.equal(prepared.report.warningCount, 1);
assert.equal(prepared.report.canProceedToPersistence, true);
```

Also parse a CSV with an unsupported header and assert `prepareRecordStaging`, not `parseCsvRecords`, rejects the schema contract. This proves responsibility remains in staging.

- [ ] **Step 2: Run the integration test**

Run:

```bash
npm run build && node --test tests/integration/csv-staging.test.mjs
```

Expected: PASS once Tasks 1-2 are complete.

- [ ] **Step 3: Run the full in-memory regression suite**

Run:

```bash
npm run test:integration
npm run verify
```

Expected: all existing and new tests PASS.

- [ ] **Step 4: Commit integration coverage**

```bash
git add tests/integration/csv-staging.test.mjs
git commit -m "test: verify CSV staging composition"
```

---

### Task 4: Documentation and authoritative verification

**Files:**
- Modify: `README.md`
- Modify: `VERIFICATION.txt`

**Interfaces:**
- Consumes: completed adapter and observed test results.
- Produces: operator/developer documentation and a truthful verification record.

- [ ] **Step 1: Document the CSV adapter**

Add a README section showing:

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

State explicitly that V0.4 is UTF-8 comma-separated CSV only and that the adapter performs CSV structural parsing, not schema/domain validation.

- [ ] **Step 2: Update local verification truthfully**

Record the exact observed typecheck, unit, and integration counts after running them. Mark the PostgreSQL GitHub Actions result for the CSV feature as pending until the feature/PR workflow actually succeeds.

- [ ] **Step 3: Run fresh final local verification**

Run:

```bash
npm run verify
node --check tests/postgres/live-postgres.test.mjs
bash -n scripts/run-postgres-integration.sh
git diff --check
```

Expected: all commands PASS.

- [ ] **Step 4: Commit docs and verification record**

```bash
git add README.md VERIFICATION.txt
git commit -m "docs: document CSV adapter verification"
```

- [ ] **Step 5: Push feature branch and use GitHub Actions as the authoritative regression gate**

Push `feature/csv-adapter`. Confirm the `PostgreSQL integration gate` completes successfully on the exact feature head. The existing live PostgreSQL behavior need not change; this gate proves the new runtime dependency and adapter did not regress migration, persistence, or lifecycle/query behavior.

- [ ] **Step 6: Record the exact successful Actions run before merge**

After the feature workflow succeeds, update `VERIFICATION.txt` with the exact run ID, feature commit SHA, and PASS result. Re-run the verification-only workflow if that documentation commit triggers CI, then open the PR to `main` and require the PR-triggered gate to pass before merge.
