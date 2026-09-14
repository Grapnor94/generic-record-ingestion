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
