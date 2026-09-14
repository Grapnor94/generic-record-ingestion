import { parse } from "csv-parse/sync";

export type ParsedCsvRecords = {
  headers: string[];
  rows: Record<string, string>[];
};

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

export function parseCsvRecords(input: string): ParsedCsvRecords {
  const source = input.startsWith("\uFEFF") ? input.slice(1) : input;
  const records = parseRecords(source);

  if (records.length === 0) {
    throw new Error("CSV parse failed: input does not contain a header row.");
  }

  const headers = records[0];
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
