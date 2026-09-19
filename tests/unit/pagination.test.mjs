import test from "node:test";
import assert from "node:assert/strict";
import {
  DEFAULT_PAGE_SIZE,
  MAX_PAGE_SIZE,
  decodeIssueCursor,
  decodeRowCursor,
  encodeIssueCursor,
  encodeRowCursor,
  validatePageSize,
} from "../../dist/db/pagination.js";
import { FrameworkError } from "../../dist/errors.js";

function encodedJson(value) {
  return Buffer.from(JSON.stringify(value), "utf8").toString("base64url");
}

function hasCode(code) {
  return error => error instanceof FrameworkError && error.code === code;
}

test("row cursors round-trip a safe integer position as unpadded base64url", () => {
  const cursor = encodeRowCursor(9007199254740991);
  assert.match(cursor, /^[A-Za-z0-9_-]+$/);
  assert.doesNotMatch(cursor, /=/);
  assert.deepEqual(decodeRowCursor(cursor), { rowNumber: 9007199254740991 });
});

test("issue cursors round-trip null and numbered row positions", () => {
  const nullRowCursor = encodeIssueCursor(null, 17);
  const numberedRowCursor = encodeIssueCursor(23, 9007199254740991);
  assert.match(nullRowCursor, /^[A-Za-z0-9_-]+$/);
  assert.match(numberedRowCursor, /^[A-Za-z0-9_-]+$/);
  assert.deepEqual(decodeIssueCursor(nullRowCursor), { rowNumber: null, issueId: 17 });
  assert.deepEqual(decodeIssueCursor(numberedRowCursor), {
    rowNumber: 23,
    issueId: 9007199254740991,
  });
});

test("cursor output uses UTF-8 base64url semantics independent of host Unicode APIs", () => {
  assert.equal(
    encodeRowCursor(7),
    Buffer.from('{"v":1,"kind":"ROW","rowNumber":7}', "utf8").toString("base64url"),
  );
  assert.equal(
    encodeIssueCursor(null, 9),
    Buffer.from('{"v":1,"kind":"ISSUE","rowNumber":null,"issueId":9}', "utf8").toString("base64url"),
  );
});

test("cursor decoders reject malformed base64url and malformed JSON", () => {
  for (const cursor of ["$$$", "abcde", Buffer.from("{", "utf8").toString("base64url")]) {
    assert.throws(() => decodeRowCursor(cursor), hasCode("INVALID_CURSOR"));
    assert.throws(() => decodeIssueCursor(cursor), hasCode("INVALID_CURSOR"));
  }
});

test("cursor decoders reject unsupported versions and the wrong cursor kind", () => {
  assert.throws(
    () => decodeRowCursor(encodedJson({ v: 2, kind: "ROW", rowNumber: 1 })),
    hasCode("INVALID_CURSOR"),
  );
  assert.throws(
    () => decodeIssueCursor(encodedJson({ v: 2, kind: "ISSUE", rowNumber: null, issueId: 1 })),
    hasCode("INVALID_CURSOR"),
  );
  assert.throws(
    () => decodeRowCursor(encodedJson({ v: 1, kind: "ISSUE", rowNumber: 1, issueId: 1 })),
    hasCode("INVALID_CURSOR"),
  );
  assert.throws(
    () => decodeIssueCursor(encodedJson({ v: 1, kind: "ROW", rowNumber: 1 })),
    hasCode("INVALID_CURSOR"),
  );
});

test("cursor decoders reject non-safe-integer positions", () => {
  const invalidRowNumbers = [1.5, 9007199254740992, "1", null];
  for (const rowNumber of invalidRowNumbers) {
    assert.throws(
      () => decodeRowCursor(encodedJson({ v: 1, kind: "ROW", rowNumber })),
      hasCode("INVALID_CURSOR"),
    );
  }

  for (const issueId of [1.5, 9007199254740992, "1", null]) {
    assert.throws(
      () => decodeIssueCursor(encodedJson({ v: 1, kind: "ISSUE", rowNumber: null, issueId })),
      hasCode("INVALID_CURSOR"),
    );
  }
  for (const rowNumber of [1.5, 9007199254740992, "1"]) {
    assert.throws(
      () => decodeIssueCursor(encodedJson({ v: 1, kind: "ISSUE", rowNumber, issueId: 1 })),
      hasCode("INVALID_CURSOR"),
    );
  }
});

test("page-size validation supplies the default and accepts the maximum", () => {
  assert.equal(DEFAULT_PAGE_SIZE, 100);
  assert.equal(MAX_PAGE_SIZE, 1000);
  assert.equal(validatePageSize(), 100);
  assert.equal(validatePageSize(1), 1);
  assert.equal(validatePageSize(1000), 1000);
});

test("page-size validation rejects non-finite, non-integral, and out-of-range values", () => {
  for (const pageSize of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, 1001]) {
    assert.throws(() => validatePageSize(pageSize), hasCode("INVALID_PAGE_SIZE"));
  }
});
