# CSV Adapter Design

Date: 2026-09-14
Status: Proposed for implementation after user review
Repository: `Grapnor94/generic-record-ingestion`
Target branch: `feature/csv-adapter`

## 1. Goal

Add a domain-neutral CSV adapter that converts UTF-8, comma-separated CSV input into the existing record-staging input shape:

```ts
{
  headers: string[];
  rows: Record<string, string>[];
}
```

The adapter sits strictly upstream of `prepareRecordStaging`. It parses CSV syntax only. It does not perform schema validation, domain transformation, record-ID extraction, diagnostics, lifecycle mutation, or database persistence.

End-to-end data flow:

```text
UTF-8 CSV input
  -> CSV adapter
  -> { headers, rows }
  -> prepareRecordStaging
  -> persistRecordStaging
  -> PostgreSQL
```

## 2. Scope

V0.4 supports only:

- UTF-8 text input;
- comma-separated CSV;
- one header row;
- RFC-style quoted fields;
- commas inside quoted fields;
- escaped double quotes inside quoted fields;
- LF and CRLF line endings;
- empty fields;
- UTF-8 BOM at the beginning of the file.

The parser will use a mature CSV parsing dependency rather than implementing quoting and escaping rules manually.

## 3. Public API

The adapter exposes one focused function:

```ts
export type ParsedCsvRecords = {
  headers: string[];
  rows: Record<string, string>[];
};

export function parseCsvRecords(input: string): ParsedCsvRecords;
```

The input is already-decoded JavaScript text. File-system reads, uploads, streams, and byte-to-text decoding remain outside this function.

The function is synchronous in V0.4 because the existing staging interface expects complete in-memory arrays and this release intentionally excludes streaming.

## 4. Parsing semantics

### 4.1 Header row

- The first CSV record is the header row.
- A UTF-8 BOM is removed only if it appears at the beginning of the input before the first header name.
- Header text is otherwise preserved exactly as decoded by the CSV parser.
- The adapter does not trim, normalize, rename, deduplicate, or validate headers.
- Header validity remains the responsibility of `prepareRecordStaging` and the existing schema-contract validator.

### 4.2 Data rows

Each subsequent CSV record becomes a plain object keyed by the parsed header names.

The adapter preserves decoded field strings exactly. It does not:

- trim whitespace;
- coerce numbers, booleans, dates, or nulls;
- collapse empty strings;
- canonicalize line endings inside quoted fields;
- perform domain-specific cleanup.

This preserves the current raw-source invariant: the staging layer receives source strings before caller-supplied transformation.

### 4.3 Empty fields

An empty CSV field becomes `""`.

Examples:

```csv
id,name,note
1,Alice,
2,,hello
```

becomes:

```ts
[
  { id: "1", name: "Alice", note: "" },
  { id: "2", name: "", note: "hello" },
]
```

### 4.4 Quoted fields

The adapter accepts standard CSV quoting behavior, including:

```csv
id,name,note
1,"Smith, Alice","He said ""hello"""
```

which decodes to:

```ts
{
  id: "1",
  name: "Smith, Alice",
  note: "He said \"hello\"",
}
```

Quoted fields may contain embedded LF or CRLF sequences as supported by the parser.

## 5. Structural validation performed by the adapter

The adapter performs only CSV-structural checks required to safely form `{ headers, rows }`.

It rejects:

- empty input with no header record;
- malformed CSV syntax, including unterminated quoted fields;
- data records whose field count differs from the header field count.

It does not reject:

- unknown headers;
- missing schema-required headers;
- duplicate or blank record IDs;
- domain-invalid values;
- warning/error conditions owned by the staging layer.

## 6. Error model

All adapter failures throw `Error` with a stable prefix:

```text
CSV parse failed: <detail>
```

Where the parser provides useful location metadata, the adapter should include a human-readable record or line location in `<detail>`.

Examples:

```text
CSV parse failed: input does not contain a header row.
CSV parse failed: row 4 has 3 fields; expected 5.
CSV parse failed: unterminated quoted field near line 7.
```

Exact parser-native wording must not leak directly into the public contract when it is unstable across dependency versions. The adapter should translate known structural failures into its own deterministic messages while preserving useful location information.

## 7. Parser dependency selection

Use a mature, actively maintained Node-compatible CSV parser with:

- strict comma-delimited parsing;
- quote/escape support;
- BOM handling or predictable pre-processing;
- row/line metadata sufficient for deterministic diagnostics;
- compatibility with Node 22 and ESM;
- no need for a framework runtime.

The implementation plan may select the concrete package after confirming its behavior in a small test spike. The selected dependency becomes a normal runtime dependency and must be pinned through `package-lock.json`.

## 8. Integration boundary

The adapter must remain independent from database and staging concerns.

Correct composition:

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

The CSV adapter must not import `persistRecordStaging`, database clients, or migration modules.

The staging layer remains authoritative for:

- contract/header validation;
- canonical transformation;
- record-ID validation;
- caller-supplied diagnostics;
- staging report generation.

The persistence layer remains authoritative for lifecycle state transitions and database writes.

## 9. Testing strategy

### 9.1 Unit tests

Add focused CSV adapter tests covering:

1. ordinary header + rows;
2. quoted comma;
3. escaped double quote;
4. LF input;
5. CRLF input;
6. UTF-8 BOM;
7. empty field preservation;
8. leading/trailing whitespace preservation;
9. embedded newline inside a quoted field;
10. empty input rejection;
11. unterminated quote rejection;
12. short row rejection;
13. long row rejection.

Tests must compare exact strings so accidental trimming/coercion is detected.

### 9.2 Integration tests

Add one adapter-to-staging test proving that parsed CSV output feeds directly into `prepareRecordStaging` and that:

- schema-contract validation still occurs in staging;
- raw source values survive unchanged into `rawSourceRow`;
- caller transformation still exclusively defines `sourceRow`;
- staging diagnostics and record-ID validation still behave as before.

### 9.3 Regression verification

Run the existing full verification suite to ensure the adapter does not alter migration, persistence, lifecycle/query, or PostgreSQL behavior.

No new database migration is expected.

## 10. Non-goals for V0.4

Explicitly excluded:

- TSV or pipe-delimited input;
- delimiter auto-detection;
- XLS/XLSX/ODS spreadsheet parsing;
- character-set detection or non-UTF-8 transcoding;
- streaming or chunked ingestion;
- very-large-file backpressure;
- file-system or HTTP upload APIs;
- header normalization;
- automatic type inference;
- schema auto-mapping;
- domain-specific field aliases;
- direct database persistence from CSV;
- retry/delete/import-management behavior.

These should be introduced only through later approved designs if concrete requirements justify them.

## 11. Success criteria

V0.4 is complete when:

1. a UTF-8 comma-separated CSV string can be parsed into exact `headers` and raw string `rows`;
2. standard quoted-field cases parse correctly;
3. malformed CSV and unequal-width rows fail with stable adapter errors;
4. BOM handling is deterministic;
5. no field trimming or type coercion occurs;
6. adapter output feeds the existing staging API without an intermediate mapping layer;
7. the full existing verification suite remains green;
8. the feature receives authoritative GitHub Actions validation before merge.

## 12. Deferred next phase

After V0.4 is merged, the next planned phase is a true file-to-database end-to-end acceptance path that composes:

```text
CSV input -> parseCsvRecords -> prepareRecordStaging -> createImportBatch -> persistRecordStaging -> query/summary verification
```

That phase should reuse this adapter unchanged rather than expanding its responsibilities.
