import { parse } from "csv-parse/sync";

export type ParsedCsvRecords = {
  headers: string[];
  rows: Record<string, string>[];
};

export class DuplicateHeaderError extends Error {
  readonly code = "DUPLICATE_HEADER";
  readonly duplicates: readonly {
    header: string;
    positions: readonly number[];
  }[];

  constructor(
    duplicates: readonly { header: string; positions: readonly number[] }[],
  ) {
    super("CSV parse failed: duplicate header names.");
    this.name = "DuplicateHeaderError";
    this.duplicates = duplicates;
  }
}

export class RowLimitExceededError extends Error {
  readonly code = "ROW_LIMIT_EXCEEDED";

  constructor(
    readonly dataRowCount: number,
    readonly maxDataRows: number,
  ) {
    super(`CSV data row count ${dataRowCount} exceeds maxDataRows ${maxDataRows}.`);
    this.name = "RowLimitExceededError";
  }
}

type CsvParserError = Error & {
  code?: string;
  lines?: number;
};

function parseRecords(source: string): string[][] {
  try {
    return parse(source, {
      delimiter: ",",
      relax_column_count: true,
    }) as string[][];
  } catch (error) {
    if (error instanceof Error) {
      const parserError = error as CsvParserError;
      if (
        parserError.code === "CSV_QUOTE_NOT_CLOSED" ||
        /quote not closed/i.test(parserError.message)
      ) {
        const location =
          typeof parserError.lines === "number"
            ? ` near line ${parserError.lines}`
            : "";
        throw new Error(
          `CSV parse failed: unterminated quoted field${location}.`,
        );
      }
    }

    throw new Error("CSV parse failed: invalid CSV syntax.");
  }
}

export function parseCsvRecords(
  input: string,
  options?: { maxDataRows: number },
): ParsedCsvRecords {
  const source = input.startsWith("\uFEFF") ? input.slice(1) : input;
  const records = parseRecords(source);

  if (records.length === 0) {
    throw new Error("CSV parse failed: input does not contain a header row.");
  }

  const headers = records[0];
  const positionsByHeader = new Map<string, number[]>();
  headers.forEach((header, index) => {
    const positions = positionsByHeader.get(header);
    if (positions) {
      positions.push(index + 1);
    } else {
      positionsByHeader.set(header, [index + 1]);
    }
  });
  const duplicates = [...positionsByHeader].flatMap(([header, positions]) =>
    positions.length > 1 ? [{ header, positions }] : [],
  );
  if (duplicates.length > 0) {
    throw new DuplicateHeaderError(duplicates);
  }

  const dataRowCount = records.length - 1;
  if (options && dataRowCount > options.maxDataRows) {
    throw new RowLimitExceededError(dataRowCount, options.maxDataRows);
  }

  const rows = records.slice(1).map((record, index) => {
    if (record.length !== headers.length) {
      throw new Error(
        `CSV parse failed: row ${index + 2} has ${record.length} fields; expected ${headers.length}.`,
      );
    }

    const row: Record<string, string> = {};
    headers.forEach((header, columnIndex) => {
      row[header] = record[columnIndex];
    });
    return row;
  });

  return { headers, rows };
}
