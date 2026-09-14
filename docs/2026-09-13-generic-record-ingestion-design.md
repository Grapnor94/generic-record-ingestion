# Generic Record Ingestion Design

## Purpose

Build a reusable, domain-neutral ingestion framework for structured record imports. The framework validates a declared schema, preserves raw records losslessly, derives a separate canonical representation through caller-supplied transformation logic, collects blocking errors and non-blocking warnings, and persists raw and canonical representations separately without changing downstream publication semantics.

## Scope

This design is intentionally domain-agnostic. It does not implement political profiling, party affiliation handling, vote-history handling, persuasion scoring, or other individualized political attributes.

The framework covers:

1. schema-contract validation;
2. raw-record preservation;
3. caller-supplied canonical transformation;
4. caller-supplied diagnostic generation;
5. generic identity/key validation;
6. raw + canonical staging persistence;
7. warning/error persistence;
8. deterministic staging reports.

## Architectural Direction

Use a generic staging core that future import adapters can call. The core owns orchestration but not domain semantics.

### Core responsibilities

The generic core:

- validates incoming headers against a supplied contract;
- creates an immutable-by-convention copy of each raw record;
- invokes a supplied transformer to create canonical data;
- invokes supplied diagnostic functions to produce warnings or errors;
- invokes a supplied identity/key extractor and validator;
- aggregates a deterministic batch report;
- hands prepared rows to persistence infrastructure.

### Caller responsibilities

A caller supplies:

- schema identifier;
- required and optional headers;
- canonical transformer;
- identity/key extractor;
- optional diagnostic functions;
- optional domain-specific validation callbacks.

The core never decodes or infers undocumented domain meanings.

## Proposed Types

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
```

A generic preparation entry point should accept a contract plus functions rather than importing any domain-specific module:

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

## Header Validation

Header validation is membership-based by default:

- every required header must be present;
- optional headers may be present;
- unknown headers are rejected;
- physical column order is not used as a validation condition unless a future caller adds an explicit ordered-header policy.

Header validation occurs before any row transformation or persistence state transition.

## Raw and Canonical Separation

Each parsed row is copied before transformation.

`rawSourceRow` preserves source values exactly at the field-value level, including whitespace and unmapped fields. `sourceRow` contains only canonical fields emitted by the caller-supplied transformer.

The generic core must not:

- trim or rewrite raw values;
- copy arbitrary raw-only fields into canonical data;
- infer missing values;
- coerce ambiguous values unless the supplied transformer explicitly does so.

## Diagnostics

Diagnostics use two severities:

- `ERROR`: blocks clean staging/persistence and marks the affected row invalid;
- `WARNING`: is persisted for review but does not invalidate an otherwise valid row.

Diagnostics are data, not exceptions, unless the caller's transform function cannot produce a usable record at all. Transformation exceptions are wrapped with the row number so failures are traceable.

The staging report derives `canProceedToPersistence` from the absence of blocking errors.

## Identity and Key Validation

The generic layer uses a caller-supplied record identifier.

Required generic checks:

- missing/blank record identifier -> `ERROR`;
- duplicate record identifier within the incoming batch -> `ERROR`.

No domain-specific identity semantics belong in the core.

## Persistence Integration

The generic persistence model stores canonical `source_row` JSON and a separate nullable `raw_source_row` JSON object.

Persistence rules:

- `source_row` remains canonical;
- `raw_source_row` stores the lossless raw record when provided;
- existing rows remain valid because `raw_source_row` is nullable;
- warnings and errors are stored in a generic issue table with `ERROR`/`WARNING` severity;
- warning diagnostics do not affect row validity or batch failure state;
- downstream publication behavior remains outside the scope of this feature.

## Data Flow

```text
parsed records
  -> header validation
  -> raw copy
  -> caller transform
  -> caller diagnostics
  -> identifier extraction
  -> generic missing/duplicate key validation
  -> staging report
  -> canonical + raw persistence
  -> issue persistence
```

## Error Handling

- Invalid headers fail before row processing.
- Transform exceptions include the 1-based row number.
- Missing or duplicate record IDs generate blocking diagnostics.
- Warning diagnostics never throw.
- No error path may mutate the caller-provided raw record.
- Database writes must be transactionally consistent.

## Testing Strategy

All tests use synthetic, non-political data only.

Example synthetic fields may include:

- `record_id`;
- `first_name`;
- `last_name`;
- `status`;
- `region`;
- arbitrary raw-only history columns unrelated to politics.

Required tests:

- exact required headers accepted;
- missing required header rejected;
- unknown header rejected;
- raw record copied and preserved exactly;
- canonical record contains only transformer output;
- warning diagnostics counted but non-blocking;
- error diagnostics blocking;
- missing record ID blocking;
- duplicate record ID blocking;
- transform exception includes row number;
- nullable `raw_source_row` migration works;
- canonical and raw rows persist separately;
- warnings persist as `WARNING`;
- existing generic reconciliation/import tests remain green.

## Scope Boundaries

This implementation must not:

- add political attributes to generic fixtures or types;
- add party/vote-history/persuasion mappings;
- modify domain-specific adapter enablement states;
- introduce a plugin system beyond the function-based interfaces required here;
- refactor unrelated code.

## Release Gate

The feature is complete only when:

- implementation follows TDD with explicit RED then GREEN evidence;
- lint, typecheck, unit, integration, and build checks pass;
- the migration is backward-compatible;
- code review finds no critical or important issues;
- the final diff contains only generic ingestion infrastructure and synthetic tests.
