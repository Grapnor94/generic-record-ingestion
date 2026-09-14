import { parse } from "csv-parse/sync";

export type ParsedCsvRecords = {
  headers: string[];
  rows: Record<string, string>[];
};

export function parseCsvRecords(input: string): ParsedCsvRecords {
  const source = input.startsWith("\uFEFF") ? input.slice(1) : input;
  const records = parse(source, {
    delimiter: ",",
    relax_column_count: true,
  }) as string[][];

  const headers = records[0] as string[];
  const rows = records.slice(1).map((record) => {
    const row: Record<string, string> = {};
    headers.forEach((header, index) => {
      row[header] = record[index] ?? "";
    });
    return row;
  });

  return { headers, rows };
}
