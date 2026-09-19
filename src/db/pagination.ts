import { FrameworkError } from "../errors.js";

export const DEFAULT_PAGE_SIZE = 100;
export const MAX_PAGE_SIZE = 1000;

export type Page<Item> = {
  items: Item[];
  nextCursor: string | null;
};

export type RowCursor = {
  rowNumber: number;
};

export type IssueCursor = {
  rowNumber: number | null;
  issueId: number;
};

type CursorPayload = Record<string, unknown>;

function invalidCursor(): FrameworkError {
  return new FrameworkError("INVALID_CURSOR", "Invalid pagination cursor.");
}

function encodeCursor(payload: CursorPayload): string {
  return Buffer.from(JSON.stringify(payload), "utf8").toString("base64url");
}

function decodeCursor(cursor: string): CursorPayload {
  if (
    typeof cursor !== "string"
    || cursor.length === 0
    || !/^[A-Za-z0-9_-]+$/.test(cursor)
    || cursor.length % 4 === 1
  ) {
    throw invalidCursor();
  }

  try {
    const bytes = Buffer.from(cursor, "base64url");
    if (bytes.toString("base64url") !== cursor) {
      throw invalidCursor();
    }
    const payload: unknown = JSON.parse(bytes.toString("utf8"));
    if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
      throw invalidCursor();
    }
    return payload as CursorPayload;
  } catch (error) {
    if (error instanceof FrameworkError) {
      throw error;
    }
    throw invalidCursor();
  }
}

function isSafeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value);
}

export function encodeRowCursor(rowNumber: number): string {
  if (!isSafeInteger(rowNumber)) {
    throw invalidCursor();
  }
  return encodeCursor({ v: 1, kind: "ROW", rowNumber });
}

export function decodeRowCursor(cursor: string): RowCursor {
  const payload = decodeCursor(cursor);
  if (payload.v !== 1 || payload.kind !== "ROW" || !isSafeInteger(payload.rowNumber)) {
    throw invalidCursor();
  }
  return { rowNumber: payload.rowNumber };
}

export function encodeIssueCursor(rowNumber: number | null, issueId: number): string {
  if ((rowNumber !== null && !isSafeInteger(rowNumber)) || !isSafeInteger(issueId)) {
    throw invalidCursor();
  }
  return encodeCursor({ v: 1, kind: "ISSUE", rowNumber, issueId });
}

export function decodeIssueCursor(cursor: string): IssueCursor {
  const payload = decodeCursor(cursor);
  if (
    payload.v !== 1
    || payload.kind !== "ISSUE"
    || (payload.rowNumber !== null && !isSafeInteger(payload.rowNumber))
    || !isSafeInteger(payload.issueId)
  ) {
    throw invalidCursor();
  }
  return { rowNumber: payload.rowNumber, issueId: payload.issueId };
}

export function validatePageSize(pageSize: number | undefined = DEFAULT_PAGE_SIZE): number {
  if (!Number.isFinite(pageSize) || !Number.isInteger(pageSize) || pageSize < 1 || pageSize > MAX_PAGE_SIZE) {
    throw new FrameworkError(
      "INVALID_PAGE_SIZE",
      `Page size must be an integer between 1 and ${MAX_PAGE_SIZE}.`,
    );
  }
  return pageSize;
}
