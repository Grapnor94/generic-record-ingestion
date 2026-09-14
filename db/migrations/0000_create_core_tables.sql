create table import_batch (
  import_id text primary key,
  schema_version text not null,
  status text not null check (status in ('RECEIVED', 'VALIDATING', 'VALIDATED', 'FAILED')),
  created_at timestamptz not null default current_timestamp,
  updated_at timestamptz not null default current_timestamp
);

create table import_stage_row (
  import_id text not null references import_batch(import_id) on delete cascade,
  row_number bigint not null,
  record_id text,
  source_row jsonb not null,
  validation_status text not null check (validation_status in ('PENDING', 'VALID', 'INVALID')),
  primary key (import_id, row_number)
);

create table import_issue (
  issue_id bigserial primary key,
  import_id text not null references import_batch(import_id) on delete cascade,
  row_number bigint,
  record_id text,
  issue_code text not null,
  severity text not null check (severity in ('ERROR', 'WARNING')),
  field_key text,
  detail text not null
);
