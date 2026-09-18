import test from "node:test";
import assert from "node:assert/strict";
import { parseCsvRecords } from "../../dist/csv/parse-csv-records.js";

test("parses ordinary comma-separated rows", () => {
  assert.deepEqual(parseCsvRecords("id,name\n1,Alice\n2,Bob\n"), {
    headers: ["id", "name"],
    rows: [
      { id: "1", name: "Alice" },
      { id: "2", name: "Bob" },
    ],
  });
});

test("rejects an exact duplicate physical header with one-based positions", () => {
  assert.throws(
    () => parseCsvRecords("Name,Name,id\nfirst,last,1\n"),
    error => error.code === "DUPLICATE_HEADER" &&
      assert.deepEqual(error.duplicates, [{ header: "Name", positions: [1, 2] }]) === undefined,
  );
});

test("reports every duplicate group in first physical occurrence order", () => {
  assert.throws(
    () => parseCsvRecords("a,b,a,b,a\n1,2,3,4,5\n"),
    error => error.code === "DUPLICATE_HEADER" &&
      assert.deepEqual(error.duplicates, [
        { header: "a", positions: [1, 3, 5] },
        { header: "b", positions: [2, 4] },
      ]) === undefined,
  );
});

test("treats case and whitespace variants as distinct physical headers", () => {
  assert.doesNotThrow(() => parseCsvRecords("Name,name,Name \n1,2,3\n"));
});

test("detects duplicate headers before considering invalid data-row widths", () => {
  assert.throws(
    () => parseCsvRecords("id,name,name\n1\n"),
    error => error.code === "DUPLICATE_HEADER" &&
      assert.deepEqual(error.duplicates, [{ header: "name", positions: [2, 3] }]) === undefined,
  );
});

test("parses quoted commas and escaped double quotes", () => {
  assert.deepEqual(
    parseCsvRecords('id,name,note\n1,"Smith, Alice","He said ""hello"""\n'),
    {
      headers: ["id", "name", "note"],
      rows: [
        { id: "1", name: "Smith, Alice", note: 'He said "hello"' },
      ],
    },
  );
});

test("supports CRLF and strips only an initial UTF-8 BOM", () => {
  assert.deepEqual(parseCsvRecords("\uFEFFid,value\r\n1,  keep me  \r\n"), {
    headers: ["id", "value"],
    rows: [{ id: "1", value: "  keep me  " }],
  });
});

test("preserves empty fields and surrounding whitespace", () => {
  assert.deepEqual(parseCsvRecords("id,name,note\n1, Alice ,\n2,, hello \n"), {
    headers: ["id", "name", "note"],
    rows: [
      { id: "1", name: " Alice ", note: "" },
      { id: "2", name: "", note: " hello " },
    ],
  });
});

test("preserves embedded newlines inside quoted fields", () => {
  assert.deepEqual(parseCsvRecords('id,note\n1,"line one\nline two"\n'), {
    headers: ["id", "note"],
    rows: [{ id: "1", note: "line one\nline two" }],
  });
});

test("rejects empty input without a header row", () => {
  assert.throws(
    () => parseCsvRecords(""),
    /^Error: CSV parse failed: input does not contain a header row\.$/,
  );
});

test("rejects short data rows with deterministic width detail", () => {
  assert.throws(
    () => parseCsvRecords("id,name\n1\n"),
    /^Error: CSV parse failed: row 2 has 1 fields; expected 2\.$/,
  );
});

test("rejects long data rows with deterministic width detail", () => {
  assert.throws(
    () => parseCsvRecords("id,name\n1,Alice,extra\n"),
    /^Error: CSV parse failed: row 2 has 3 fields; expected 2\.$/,
  );
});

test("translates unterminated quoted fields into a stable adapter error", () => {
  assert.throws(
    () => parseCsvRecords('id,note\n1,"unterminated'),
    /^Error: CSV parse failed: .*unterminated quoted field.*$/i,
  );
});
