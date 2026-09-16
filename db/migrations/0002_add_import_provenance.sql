alter table import_batch
  add column source_kind text check (source_kind in ('CSV_TEXT', 'LOCAL_FILE')),
  add column source_name text,
  add column source_size_bytes bigint check (source_size_bytes between 0 and 9007199254740991),
  add column source_sha256 text check (source_sha256 ~ '^[0-9a-f]{64}$'),
  add column source_path text;
