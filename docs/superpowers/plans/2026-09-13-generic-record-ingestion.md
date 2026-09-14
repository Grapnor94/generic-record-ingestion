# Generic Record Ingestion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a reusable, domain-neutral record-ingestion framework that validates declared schemas, preserves raw rows losslessly, derives canonical rows through caller-supplied functions, persists generic warnings/errors, and stores raw and canonical records separately.

**Architecture:** Introduce a small generic staging core with function-based dependency injection for transformation, identifier extraction, and diagnostics. Keep persistence transactional and backward-compatible by adding a nullable raw-source JSON column while leaving downstream publication outside this module. The framework remains domain-neutral and uses only synthetic, non-political tests.

**Tech Stack:** Node.js 22+, TypeScript 5.9, Vitest 3.2, PostgreSQL 18, existing project lint/typecheck/build tooling.

**Spec:** `2026-09-13-generic-record-ingestion-design.md`

## Global Constraints

- The implementation is domain-agnostic and must not add political profiling, party affiliation, vote-history handling, persuasion scoring, or other individualized political attributes.
- Use function-based interfaces only; do not introduce a general plugin framework.
- Raw source values must be preserved exactly at the field-value level.
- Canonical rows contain only caller-transformer output.
- Unknown headers are rejected; header order is not significant.
- `ERROR` diagnostics block clean staging; `WARNING` diagnostics do not.
- Missing/blank and duplicate record identifiers are generic blocking errors.
- `raw_source_row` must be nullable for backward compatibility.
- Database writes must remain transactionally consistent.
- All fixtures and tests use synthetic, non-political data only.
- Do not refactor unrelated code or alter downstream publication semantics.

---

## File Structure

Create a focused standalone module with the following responsibilities:

- `src/ingestion/types.ts` — shared generic contracts and result types only.
- `src/ingestion/validate-headers.ts` — required/optional/unknown header validation.
- `src/ingestion/validate-record-ids.ts` — generic missing/duplicate ID diagnostics.
- `src/ingestion/prepare-record-staging.ts` — orchestration of raw-copy, transform, diagnostics, ID extraction, and report aggregation.
- `src/ingestion/persist-record-staging.ts` — transactional persistence of canonical/raw records and diagnostics.
- `db/migrations/0001_add_raw_source_row.sql` — backward-compatible nullable raw row storage.
- `tests/unit/validate-headers.test.ts` — schema-contract behavior.
- `tests/unit/validate-record-ids.test.ts` — identity validation behavior.
- `tests/unit/prepare-record-staging.test.ts` — orchestration behavior.
- `tests/integration/persist-record-staging.test.ts` — database migration and persistence behavior.
- `tests/fixtures/synthetic-records.ts` — reusable neutral fixtures only.

If integrating into an existing codebase with different directory conventions, preserve these responsibilities while adapting paths to the local conventions before implementation begins.

---

### Task 1: Define the generic ingestion contracts and header validation

**Files:**
- Create: `src/ingestion/types.ts`
- Create: `src/ingestion/validate-headers.ts`
- Create: `tests/unit/validate-headers.test.ts`

**Interfaces:**
- Produces:
```ts
export type RecordSchemaContract = {
  schemaVersion: string;
  requiredHeaders: readonly string[];
  optionalHeaders?: readonly string[];
};

export type StagingDiagnostic = {
  code: string;
  severity: "ERROR" | "WARNING";
  fieldKey?: string;
  detail: string;
};

export type PreparedRecord = {
  rowNumber: number;
  recordId: string | null;
  rawSourceRow: Record<string, string>;
  sourceRow: Record<string, unknown>;
  diagnostics: StagingDiagnostic[];
};

export type StagingReport = {
  schemaVersion: string;
  headerStatus: "VALID";
  rowCount: number;
  warningCount: number;
  errorCount: number;
  rowsWithDiagnostics: number;
  canProceedToPersistence: boolean;
};

export class UnsupportedRecordSchemaError extends Error {}

export function assertSupportedRecordHeaders(
  contract: RecordSchemaContract,
  headers: readonly string[],
): void;
```

- [ ] **Step 1: Write RED tests for accepted, missing, and unknown headers**

Create `tests/unit/validate-headers.test.ts`:

```ts
import { describe, expect, it } from "vitest";
import { assertSupportedRecordHeaders } from "../../src/ingestion/validate-headers";

const contract = {
  schemaVersion: "GENERIC_V1",
  requiredHeaders: ["record_id", "first_name", "status"],
  optionalHeaders: ["region"],
} as const;

describe("assertSupportedRecordHeaders", () => {
  it("accepts all required headers plus declared optional headers", () => {
    expect(() =>
      assertSupportedRecordHeaders(contract, [
        "record_id",
        "first_name",
        "status",
        "region",
      ]),
    ).not.toThrow();
  });

  it("accepts equivalent headers in a different physical order", () => {
    expect(() =>
      assertSupportedRecordHeaders(contract, [
        "status",
        "record_id",
        "first_name",
      ]),
    ).not.toThrow();
  });

  it("rejects a missing required header", () => {
    expect(() =>
      assertSupportedRecordHeaders(contract, ["record_id", "first_name"]),
    ).toThrow(/missing required columns: status/i);
  });

  it("rejects an unknown header", () => {
    expect(() =>
      assertSupportedRecordHeaders(contract, [
        "record_id",
        "first_name",
        "status",
        "unexpected",
      ]),
    ).toThrow(/unknown columns: unexpected/i);
  });
});
```

- [ ] **Step 2: Run the focused test and verify RED**

Run:
```bash
npm test -- tests/unit/validate-headers.test.ts
```
Expected: FAIL because `src/ingestion/validate-headers.ts` does not exist.

- [ ] **Step 3: Add shared types**

Create `src/ingestion/types.ts` with exactly the type definitions in the Interfaces block. Do not add domain-specific fields.

- [ ] **Step 4: Implement minimal membership-based header validation**

Create `src/ingestion/validate-headers.ts`:

```ts
import type { RecordSchemaContract } from "./types";

export class UnsupportedRecordSchemaError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "UnsupportedRecordSchemaError";
  }
}

export function assertSupportedRecordHeaders(
  contract: RecordSchemaContract,
  headers: readonly string[],
): void {
  const actual = new Set(headers);
  const allowed = new Set([
    ...contract.requiredHeaders,
    ...(contract.optionalHeaders ?? []),
  ]);

  const missing = contract.requiredHeaders.filter((header) => !actual.has(header));
  const unknown = headers.filter((header) => !allowed.has(header));

  if (missing.length === 0 && unknown.length === 0) return;

  const problems: string[] = [];
  if (missing.length > 0) {
    problems.push(`missing required columns: ${missing.join(", ")}`);
  }
  if (unknown.length > 0) {
    problems.push(`unknown columns: ${unknown.join(", ")}`);
  }

  throw new UnsupportedRecordSchemaError(
    `Record schema ${contract.schemaVersion} is not supported (${problems.join("; ")}).`,
  );
}
```

- [ ] **Step 5: Run Task 1 tests and typecheck**

Run:
```bash
npm test -- tests/unit/validate-headers.test.ts
npm run typecheck
```
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/ingestion/types.ts src/ingestion/validate-headers.ts tests/unit/validate-headers.test.ts
git commit -m "feat: add generic ingestion schema validation"
```

---

### Task 2: Add generic record-ID validation

**Files:**
- Create: `src/ingestion/validate-record-ids.ts`
- Create: `tests/unit/validate-record-ids.test.ts`
- Reuse: `src/ingestion/types.ts`

**Interfaces:**
- Produces:
```ts
export function validateRecordIds(
  rows: Array<Pick<PreparedRecord, "rowNumber" | "recordId">>,
): StagingDiagnostic[];
```
- Missing/blank ID diagnostic:
```ts
{
  code: "MISSING_RECORD_ID",
  severity: "ERROR",
  fieldKey: "record_id",
  detail: "Record identifier is required.",
}
```
- Duplicate ID diagnostic:
```ts
{
  code: "DUPLICATE_RECORD_ID",
  severity: "ERROR",
  fieldKey: "record_id",
  detail: `Duplicate record identifier ${recordId} within the incoming batch.`,
}
```

- [ ] **Step 1: Write RED tests for missing and duplicate IDs**

Create `tests/unit/validate-record-ids.test.ts`:

```ts
import { describe, expect, it } from "vitest";
import { validateRecordIds } from "../../src/ingestion/validate-record-ids";

describe("validateRecordIds", () => {
  it("reports a blank record ID as an error", () => {
    expect(
      validateRecordIds([{ rowNumber: 1, recordId: "   " }]),
    ).toEqual([
      {
        code: "MISSING_RECORD_ID",
        severity: "ERROR",
        fieldKey: "record_id",
        detail: "Record identifier is required.",
      },
    ]);
  });

  it("reports only the later duplicate occurrence", () => {
    expect(
      validateRecordIds([
        { rowNumber: 1, recordId: "A-1" },
        { rowNumber: 2, recordId: "A-1" },
      ]),
    ).toEqual([
      {
        code: "DUPLICATE_RECORD_ID",
        severity: "ERROR",
        fieldKey: "record_id",
        detail: "Duplicate record identifier A-1 within the incoming batch.",
      },
    ]);
  });

  it("returns no diagnostics for unique nonblank IDs", () => {
    expect(
      validateRecordIds([
        { rowNumber: 1, recordId: "A-1" },
        { rowNumber: 2, recordId: "A-2" },
      ]),
    ).toEqual([]);
  });
});
```

- [ ] **Step 2: Run focused tests and verify RED**

Run:
```bash
npm test -- tests/unit/validate-record-ids.test.ts
```
Expected: FAIL because `validateRecordIds` does not exist.

- [ ] **Step 3: Implement minimal ID validation**

Create `src/ingestion/validate-record-ids.ts`:

```ts
import type { PreparedRecord, StagingDiagnostic } from "./types";

export function validateRecordIds(
  rows: Array<Pick<PreparedRecord, "rowNumber" | "recordId">>,
): StagingDiagnostic[] {
  const diagnostics: StagingDiagnostic[] = [];
  const seen = new Set<string>();

  for (const row of rows) {
    const recordId = row.recordId?.trim() || null;
    if (!recordId) {
      diagnostics.push({
        code: "MISSING_RECORD_ID",
        severity: "ERROR",
        fieldKey: "record_id",
        detail: "Record identifier is required.",
      });
      continue;
    }

    if (seen.has(recordId)) {
      diagnostics.push({
        code: "DUPLICATE_RECORD_ID",
        severity: "ERROR",
        fieldKey: "record_id",
        detail: `Duplicate record identifier ${recordId} within the incoming batch.`,
      });
      continue;
    }

    seen.add(recordId);
  }

  return diagnostics;
}
```

- [ ] **Step 4: Run Task 2 tests and regression Task 1 tests**

Run:
```bash
npm test -- tests/unit/validate-record-ids.test.ts tests/unit/validate-headers.test.ts
npm run typecheck
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/ingestion/validate-record-ids.ts tests/unit/validate-record-ids.test.ts
git commit -m "feat: add generic record identity validation"
```

---

### Task 3: Build the staging orchestrator and deterministic report

**Files:**
- Create: `src/ingestion/prepare-record-staging.ts`
- Create: `tests/unit/prepare-record-staging.test.ts`
- Create: `tests/fixtures/synthetic-records.ts`
- Reuse: `src/ingestion/types.ts`
- Reuse: `src/ingestion/validate-headers.ts`
- Reuse: `src/ingestion/validate-record-ids.ts`

**Interfaces:**
- Produces:
```ts
export function prepareRecordStaging(input: {
  contract: RecordSchemaContract;
  headers: readonly string[];
  rows: readonly Record<string, string>[];
  transform: (row: Record<string, string>) => Record<string, unknown>;
  getRecordId: (
    row: Record<string, string>,
    canonical: Record<string, unknown>,
  ) => string | null;
  diagnose?: (
    row: Record<string, string>,
    canonical: Record<string, unknown>,
  ) => StagingDiagnostic[];
}): {
  rows: PreparedRecord[];
  report: StagingReport;
};
```

- [ ] **Step 1: Create neutral synthetic fixtures**

Create `tests/fixtures/synthetic-records.ts`:

```ts
export const GENERIC_HEADERS = [
  "record_id",
  "first_name",
  "last_name",
  "status",
  "region",
  "legacy_history_code",
] as const;

export const GENERIC_CONTRACT = {
  schemaVersion: "GENERIC_V1",
  requiredHeaders: ["record_id", "first_name", "last_name", "status"],
  optionalHeaders: ["region", "legacy_history_code"],
} as const;

export function syntheticRecord(overrides: Record<string, string> = {}) {
  return {
    record_id: " R-001 ",
    first_name: " Ada ",
    last_name: " Example ",
    status: " active ",
    region: " north ",
    legacy_history_code: " RAW-7 ",
    ...overrides,
  };
}
```

- [ ] **Step 2: Write RED tests for raw preservation and canonical isolation**

Create `tests/unit/prepare-record-staging.test.ts` with:

```ts
import { describe, expect, it } from "vitest";
import { prepareRecordStaging } from "../../src/ingestion/prepare-record-staging";
import {
  GENERIC_CONTRACT,
  GENERIC_HEADERS,
  syntheticRecord,
} from "../fixtures/synthetic-records";

const transform = (row: Record<string, string>) => ({
  first_name: row.first_name.trim(),
  last_name: row.last_name.trim(),
  status: row.status.trim().toUpperCase(),
  region: row.region.trim(),
});

const getRecordId = (row: Record<string, string>) => row.record_id.trim() || null;

describe("prepareRecordStaging", () => {
  it("preserves the raw row while keeping canonical output transformer-defined", () => {
    const raw = syntheticRecord();
    const result = prepareRecordStaging({
      contract: GENERIC_CONTRACT,
      headers: GENERIC_HEADERS,
      rows: [raw],
      transform,
      getRecordId,
    });

    expect(result.rows[0].rawSourceRow).toEqual(raw);
    expect(result.rows[0].rawSourceRow).not.toBe(raw);
    expect(result.rows[0].sourceRow).toEqual({
      first_name: "Ada",
      last_name: "Example",
      status: "ACTIVE",
      region: "north",
    });
    expect(result.rows[0].sourceRow).not.toHaveProperty("legacy_history_code");
    expect(raw.first_name).toBe(" Ada ");
  });
});
```

- [ ] **Step 3: Run focused test and verify RED**

Run:
```bash
npm test -- tests/unit/prepare-record-staging.test.ts
```
Expected: FAIL because `prepareRecordStaging` does not exist.

- [ ] **Step 4: Implement minimal orchestration**

Create `src/ingestion/prepare-record-staging.ts`:

```ts
import type {
  PreparedRecord,
  RecordSchemaContract,
  StagingDiagnostic,
  StagingReport,
} from "./types";
import { assertSupportedRecordHeaders } from "./validate-headers";
import { validateRecordIds } from "./validate-record-ids";

export function prepareRecordStaging(input: {
  contract: RecordSchemaContract;
  headers: readonly string[];
  rows: readonly Record<string, string>[];
  transform: (row: Record<string, string>) => Record<string, unknown>;
  getRecordId: (
    row: Record<string, string>,
    canonical: Record<string, unknown>,
  ) => string | null;
  diagnose?: (
    row: Record<string, string>,
    canonical: Record<string, unknown>,
  ) => StagingDiagnostic[];
}): { rows: PreparedRecord[]; report: StagingReport } {
  assertSupportedRecordHeaders(input.contract, input.headers);

  const rows = input.rows.map((incoming, index): PreparedRecord => {
    const rawSourceRow = { ...incoming };
    try {
      const sourceRow = input.transform(rawSourceRow);
      const recordId = input.getRecordId(rawSourceRow, sourceRow);
      const diagnostics = input.diagnose?.(rawSourceRow, sourceRow) ?? [];
      return {
        rowNumber: index + 1,
        recordId,
        rawSourceRow,
        sourceRow,
        diagnostics,
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      throw new Error(`Record staging failed at row ${index + 1}: ${message}`);
    }
  });

  const identityDiagnostics = validateRecordIds(
    rows.map(({ rowNumber, recordId }) => ({ rowNumber, recordId })),
  );

  if (identityDiagnostics.length > 0) {
    for (const diagnostic of identityDiagnostics) {
      const matchingRow = diagnostic.code === "MISSING_RECORD_ID"
        ? rows.find((row) => !row.recordId?.trim())
        : undefined;
      if (matchingRow) matchingRow.diagnostics.push(diagnostic);
    }
  }

  const allDiagnostics = rows.flatMap((row) => row.diagnostics);
  const warningCount = allDiagnostics.filter((d) => d.severity === "WARNING").length;
  const errorCount = allDiagnostics.filter((d) => d.severity === "ERROR").length;

  return {
    rows,
    report: {
      schemaVersion: input.contract.schemaVersion,
      headerStatus: "VALID",
      rowCount: rows.length,
      warningCount,
      errorCount,
      rowsWithDiagnostics: rows.filter((row) => row.diagnostics.length > 0).length,
      canProceedToPersistence: errorCount === 0,
    },
  };
}
```

Before finalizing this task, refine the identity-diagnostic mapping so each missing or duplicate error is attached to its actual row. Do this by evolving `validateRecordIds` to return `{ rowNumber, diagnostic }` rather than dropping row position. Keep diagnostic payloads unchanged.

- [ ] **Step 5: Add RED tests for warnings, errors, duplicates, and transform exceptions**

Append tests:

```ts
it("counts warnings without blocking persistence", () => {
  const result = prepareRecordStaging({
    contract: GENERIC_CONTRACT,
    headers: GENERIC_HEADERS,
    rows: [syntheticRecord({ status: " legacy " })],
    transform,
    getRecordId,
    diagnose: (row) =>
      row.status.trim() === "legacy"
        ? [{
            code: "LEGACY_STATUS",
            severity: "WARNING",
            fieldKey: "status",
            detail: "Legacy status encountered.",
          }]
        : [],
  });

  expect(result.report.warningCount).toBe(1);
  expect(result.report.errorCount).toBe(0);
  expect(result.report.canProceedToPersistence).toBe(true);
});

it("blocks persistence for caller-supplied errors", () => {
  const result = prepareRecordStaging({
    contract: GENERIC_CONTRACT,
    headers: GENERIC_HEADERS,
    rows: [syntheticRecord()],
    transform,
    getRecordId,
    diagnose: () => [{
      code: "INVALID_STATUS",
      severity: "ERROR",
      fieldKey: "status",
      detail: "Status is invalid.",
    }],
  });

  expect(result.report.errorCount).toBe(1);
  expect(result.report.canProceedToPersistence).toBe(false);
});

it("blocks duplicate record IDs", () => {
  const result = prepareRecordStaging({
    contract: GENERIC_CONTRACT,
    headers: GENERIC_HEADERS,
    rows: [
      syntheticRecord({ record_id: "R-1" }),
      syntheticRecord({ record_id: "R-1" }),
    ],
    transform,
    getRecordId,
  });

  expect(result.report.canProceedToPersistence).toBe(false);
  expect(result.rows[1].diagnostics.map((d) => d.code)).toContain(
    "DUPLICATE_RECORD_ID",
  );
});

it("includes the row number when transformation throws", () => {
  expect(() =>
    prepareRecordStaging({
      contract: GENERIC_CONTRACT,
      headers: GENERIC_HEADERS,
      rows: [syntheticRecord()],
      transform: () => {
        throw new Error("cannot normalize");
      },
      getRecordId,
    }),
  ).toThrow(/row 1: cannot normalize/i);
});
```

- [ ] **Step 6: Run Task 3 unit suite and typecheck**

Run:
```bash
npm test -- tests/unit/prepare-record-staging.test.ts tests/unit/validate-record-ids.test.ts tests/unit/validate-headers.test.ts
npm run typecheck
```
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/ingestion/prepare-record-staging.ts src/ingestion/validate-record-ids.ts tests/unit/prepare-record-staging.test.ts tests/unit/validate-record-ids.test.ts tests/fixtures/synthetic-records.ts
git commit -m "feat: add generic record staging orchestration"
```

---

### Task 4: Add backward-compatible raw-row persistence

**Files:**
- Create: `db/migrations/0001_add_raw_source_row.sql`
- Create: `src/ingestion/persist-record-staging.ts`
- Create: `tests/integration/persist-record-staging.test.ts`
- Reuse: `src/ingestion/types.ts`

**Interfaces:**
- Database assumptions for the standalone integration fixture:
```sql
create table import_batch (
  import_id uuid primary key,
  status text not null check (status in ('RECEIVED','VALIDATING','VALIDATED','FAILED'))
);

create table import_stage_row (
  import_id uuid not null references import_batch(import_id),
  row_number bigint not null,
  record_id text,
  source_row jsonb not null,
  validation_status text not null check (validation_status in ('PENDING','VALID','INVALID')),
  primary key (import_id, row_number)
);

create table import_issue (
  import_id uuid not null references import_batch(import_id),
  row_number bigint,
  record_id text,
  issue_code text not null,
  severity text not null check (severity in ('ERROR','WARNING')),
  field_key text,
  detail text not null
);
```
- Produces migration:
```sql
alter table import_stage_row
  add column if not exists raw_source_row jsonb;
```
- Produces function:
```ts
export async function persistRecordStaging(
  db: Queryable,
  input: {
    importId: string;
    rows: PreparedRecord[];
  },
): Promise<{
  status: "VALIDATED" | "FAILED";
  issueCount: number;
}>;
```

- [ ] **Step 1: Write RED integration test for nullable migration**

In `tests/integration/persist-record-staging.test.ts`, create the fixture tables above in `beforeAll`, then run the migration file and assert:

```ts
const column = await client.query(`
  select is_nullable
  from information_schema.columns
  where table_name = 'import_stage_row'
    and column_name = 'raw_source_row'
`);

expect(column.rows).toEqual([{ is_nullable: "YES" }]);
```

- [ ] **Step 2: Run integration test and verify RED**

Run:
```bash
npm run test:integration -- tests/integration/persist-record-staging.test.ts
```
Expected: FAIL because the migration file does not exist.

- [ ] **Step 3: Create the backward-compatible migration**

Create `db/migrations/0001_add_raw_source_row.sql`:

```sql
alter table import_stage_row
  add column if not exists raw_source_row jsonb;
```

- [ ] **Step 4: Add RED persistence test for raw/canonical separation and warnings**

Add a synthetic prepared row:

```ts
const row: PreparedRecord = {
  rowNumber: 1,
  recordId: "R-1",
  rawSourceRow: {
    record_id: " R-1 ",
    first_name: " Ada ",
    legacy_history_code: " RAW-7 ",
  },
  sourceRow: {
    first_name: "Ada",
  },
  diagnostics: [{
    code: "LEGACY_VALUE",
    severity: "WARNING",
    fieldKey: "legacy_history_code",
    detail: "Legacy value retained in raw source.",
  }],
};
```

Insert an `import_batch` in `RECEIVED`, call `persistRecordStaging`, and assert:

```ts
expect(result).toEqual({ status: "VALIDATED", issueCount: 1 });

const staged = await client.query(`
  select source_row, raw_source_row, validation_status
  from import_stage_row
  where import_id = $1 and row_number = 1
`, [importId]);

expect(staged.rows[0].source_row).toEqual({ first_name: "Ada" });
expect(staged.rows[0].raw_source_row).toEqual({
  record_id: " R-1 ",
  first_name: " Ada ",
  legacy_history_code: " RAW-7 ",
});
expect(staged.rows[0].validation_status).toBe("VALID");
```

Query `import_issue` and assert severity `WARNING`.

- [ ] **Step 5: Implement transactional persistence**

Create `src/ingestion/persist-record-staging.ts` with:

```ts
import type { QueryResult } from "pg";
import type { PreparedRecord } from "./types";

type Queryable = {
  query<T extends Record<string, unknown> = Record<string, unknown>>(
    text: string,
    values?: unknown[],
  ): Promise<QueryResult<T>>;
};

export async function persistRecordStaging(
  db: Queryable,
  input: { importId: string; rows: PreparedRecord[] },
): Promise<{ status: "VALIDATED" | "FAILED"; issueCount: number }> {
  await db.query("begin");
  try {
    const transition = await db.query(
      `update import_batch
       set status = 'VALIDATING'
       where import_id = $1 and status = 'RECEIVED'
       returning import_id`,
      [input.importId],
    );
    if (transition.rowCount !== 1) {
      throw new Error("Import must be in RECEIVED status before validation.");
    }

    for (const row of input.rows) {
      await db.query(
        `insert into import_stage_row (
           import_id, row_number, record_id, source_row, raw_source_row, validation_status
         ) values ($1, $2, $3, $4::jsonb, $5::jsonb, 'PENDING')`,
        [
          input.importId,
          row.rowNumber,
          row.recordId?.trim() || null,
          JSON.stringify(row.sourceRow),
          JSON.stringify(row.rawSourceRow),
        ],
      );

      for (const diagnostic of row.diagnostics) {
        await db.query(
          `insert into import_issue (
             import_id, row_number, record_id, issue_code, severity, field_key, detail
           ) values ($1, $2, $3, $4, $5, $6, $7)`,
          [
            input.importId,
            row.rowNumber,
            row.recordId?.trim() || null,
            diagnostic.code,
            diagnostic.severity,
            diagnostic.fieldKey ?? null,
            diagnostic.detail,
          ],
        );
      }
    }

    const invalidRows = input.rows
      .filter((row) => row.diagnostics.some((d) => d.severity === "ERROR"))
      .map((row) => row.rowNumber);

    await db.query(
      `update import_stage_row
       set validation_status = case
         when row_number = any($2::bigint[]) then 'INVALID'
         else 'VALID'
       end
       where import_id = $1`,
      [input.importId, invalidRows],
    );

    const hasErrors = invalidRows.length > 0;
    const status = hasErrors ? "FAILED" : "VALIDATED";
    await db.query(
      `update import_batch set status = $2 where import_id = $1`,
      [input.importId, status],
    );
    await db.query("commit");

    return {
      status,
      issueCount: input.rows.reduce((sum, row) => sum + row.diagnostics.length, 0),
    };
  } catch (error) {
    await db.query("rollback");
    throw error;
  }
}
```

- [ ] **Step 6: Add RED/GREEN test for blocking errors**

Add a row with one `ERROR` diagnostic and assert:

```ts
expect(result.status).toBe("FAILED");
expect(staged.rows[0].validation_status).toBe("INVALID");
```

Also assert a warning-only row remains `VALID`.

- [ ] **Step 7: Run Task 4 tests and typecheck**

Run:
```bash
npm run test:integration -- tests/integration/persist-record-staging.test.ts
npm run typecheck
```
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add db/migrations/0001_add_raw_source_row.sql src/ingestion/persist-record-staging.ts tests/integration/persist-record-staging.test.ts
git commit -m "feat: persist generic raw and canonical staging rows"
```

---

### Task 5: Add the end-to-end generic staging entry point

**Files:**
- Create: `src/ingestion/stage-record-import.ts`
- Create: `tests/integration/stage-record-import.test.ts`
- Reuse: `src/ingestion/prepare-record-staging.ts`
- Reuse: `src/ingestion/persist-record-staging.ts`
- Reuse: `tests/fixtures/synthetic-records.ts`

**Interfaces:**
- Produces:
```ts
export async function stageRecordImport(
  db: Queryable,
  input: {
    importId: string;
    contract: RecordSchemaContract;
    headers: readonly string[];
    rows: readonly Record<string, string>[];
    transform: (row: Record<string, string>) => Record<string, unknown>;
    getRecordId: (
      row: Record<string, string>,
      canonical: Record<string, unknown>,
    ) => string | null;
    diagnose?: (
      row: Record<string, string>,
      canonical: Record<string, unknown>,
    ) => StagingDiagnostic[];
  },
): Promise<{
  report: StagingReport;
  persistence: {
    status: "VALIDATED" | "FAILED";
    issueCount: number;
  };
}>;
```

- [ ] **Step 1: Write RED end-to-end integration test**

Create `tests/integration/stage-record-import.test.ts` that:
1. creates the neutral fixture tables;
2. applies the raw-source migration;
3. inserts one `RECEIVED` import batch;
4. calls `stageRecordImport` with two synthetic rows, one containing a warning-producing legacy status;
5. asserts:

```ts
expect(result.report.headerStatus).toBe("VALID");
expect(result.report.rowCount).toBe(2);
expect(result.report.warningCount).toBe(1);
expect(result.report.errorCount).toBe(0);
expect(result.report.canProceedToPersistence).toBe(true);
expect(result.persistence).toEqual({
  status: "VALIDATED",
  issueCount: 1,
});
```

Then query persistence and assert raw/canonical separation.

- [ ] **Step 2: Add RED test that bad headers cause no state transition**

Insert a second `RECEIVED` batch, call with missing `status` header, expect schema error, then assert:

```ts
const batch = await client.query(
  `select status from import_batch where import_id = $1`,
  [importId],
);
expect(batch.rows[0].status).toBe("RECEIVED");
```

- [ ] **Step 3: Run integration test and verify RED**

Run:
```bash
npm run test:integration -- tests/integration/stage-record-import.test.ts
```
Expected: FAIL because `stageRecordImport` does not exist.

- [ ] **Step 4: Implement orchestration-only entry point**

Create `src/ingestion/stage-record-import.ts`:

```ts
import type { QueryResult } from "pg";
import type {
  RecordSchemaContract,
  StagingDiagnostic,
  StagingReport,
} from "./types";
import { prepareRecordStaging } from "./prepare-record-staging";
import { persistRecordStaging } from "./persist-record-staging";

type Queryable = {
  query<T extends Record<string, unknown> = Record<string, unknown>>(
    text: string,
    values?: unknown[],
  ): Promise<QueryResult<T>>;
};

export async function stageRecordImport(
  db: Queryable,
  input: {
    importId: string;
    contract: RecordSchemaContract;
    headers: readonly string[];
    rows: readonly Record<string, string>[];
    transform: (row: Record<string, string>) => Record<string, unknown>;
    getRecordId: (
      row: Record<string, string>,
      canonical: Record<string, unknown>,
    ) => string | null;
    diagnose?: (
      row: Record<string, string>,
      canonical: Record<string, unknown>,
    ) => StagingDiagnostic[];
  },
): Promise<{
  report: StagingReport;
  persistence: { status: "VALIDATED" | "FAILED"; issueCount: number };
}> {
  const prepared = prepareRecordStaging(input);
  const persistence = await persistRecordStaging(db, {
    importId: input.importId,
    rows: prepared.rows,
  });
  return { report: prepared.report, persistence };
}
```

No normalization, schema logic, or diagnostic logic may be duplicated here.

- [ ] **Step 5: Run focused end-to-end regression suite**

Run:
```bash
npm test -- tests/unit/validate-headers.test.ts tests/unit/validate-record-ids.test.ts tests/unit/prepare-record-staging.test.ts
npm run test:integration -- tests/integration/persist-record-staging.test.ts tests/integration/stage-record-import.test.ts
npm run typecheck
```
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/ingestion/stage-record-import.ts tests/integration/stage-record-import.test.ts
git commit -m "feat: add generic record import staging entry point"
```

---

### Task 6: Final verification and release gate

**Files:**
- Verify: all `src/ingestion/*.ts`
- Verify: `db/migrations/0001_add_raw_source_row.sql`
- Verify: all generic ingestion tests
- Modify only if needed to fix verified defects.

**Interfaces:**
- No new interfaces.
- Final module surface should consist only of the generic contracts and functions defined in Tasks 1–5.

- [ ] **Step 1: Run full local verification**

Run:
```bash
npm audit --audit-level=high
npm run lint
npm run typecheck
npm test
npm run test:integration
npm run build
```
Expected: all PASS.

- [ ] **Step 2: Verify transactional failure behavior**

Add one integration test that injects a database error after at least one stage-row insert but before commit. Assert:

```ts
expect(stageRowsAfterFailure).toHaveLength(0);
expect(issuesAfterFailure).toHaveLength(0);
expect(batchStatusAfterFailure).toBe("RECEIVED");
```

If the database transaction semantics leave the status at `RECEIVED` because the `VALIDATING` update rolled back, preserve that behavior.

- [ ] **Step 3: Re-run integration suite after transactional test**

Run:
```bash
npm run test:integration
```
Expected: PASS.

- [ ] **Step 4: Scope-leakage review**

Inspect the final diff and confirm all of the following:

```text
[ ] No political attributes in production types
[ ] No political attributes in fixtures or tests
[ ] No party/vote-history/persuasion mapping
[ ] No domain-specific adapter enablement changes
[ ] No plugin framework beyond supplied functions
[ ] raw_source_row remains nullable
[ ] WARNING does not invalidate rows
[ ] ERROR invalidates rows and fails batch
[ ] Header validation runs before persistence state transition
[ ] Raw records remain unmodified
[ ] Canonical records contain only transformer output
```

- [ ] **Step 5: Request code review**

Use the code-review workflow against the complete branch diff. Resolve every critical or important finding. After each fix, rerun the smallest affected test set plus `npm run typecheck`.

- [ ] **Step 6: Final verification after review fixes**

Run again:
```bash
npm audit --audit-level=high
npm run lint
npm run typecheck
npm test
npm run test:integration
npm run build
```
Expected: all PASS.

- [ ] **Step 7: Final commit if review fixes changed code**

```bash
git add src/ingestion db/migrations tests
git commit -m "fix: address generic ingestion review findings"
```

Only create this commit if review fixes actually changed files.

---

## Self-Review

### Spec coverage

- Schema-contract validation: Task 1.
- Raw-record preservation and canonical separation: Task 3.
- Caller-supplied transform, ID extraction, and diagnostics: Task 3.
- Generic missing/duplicate identifier validation: Task 2 and Task 3.
- Deterministic staging report: Task 3.
- Nullable raw-source persistence: Task 4.
- Warning/error persistence and row validity semantics: Task 4.
- Header failure before state transition: Task 5.
- Transactional consistency: Task 4 and Task 6.
- Synthetic non-political tests: Tasks 1–6.
- No downstream publication or political-domain changes: Global Constraints and Task 6 scope review.

### Placeholder scan

No `TODO`, `TBD`, “implement later,” or unspecified validation/error-handling steps remain. All production interfaces referenced by later tasks are defined in earlier task interface blocks.

### Type consistency

The names `RecordSchemaContract`, `StagingDiagnostic`, `PreparedRecord`, `StagingReport`, `prepareRecordStaging`, `persistRecordStaging`, and `stageRecordImport` are used consistently across tasks. `recordId`, `rawSourceRow`, `sourceRow`, and `diagnostics` retain the same shapes throughout.
