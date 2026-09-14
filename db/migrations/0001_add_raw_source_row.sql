alter table import_stage_row
  add column if not exists raw_source_row jsonb;
