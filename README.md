# Generic Record Ingestion

A standalone, domain-neutral TypeScript ingestion framework for structured record imports.

It provides:
- required/optional header validation with unknown-column rejection;
- lossless raw-row copies kept separate from canonical transformer output;
- caller-supplied record-ID extraction and diagnostics;
- generic missing/duplicate record-ID validation;
- deterministic warning/error staging reports;
- transactional persistence of canonical and raw JSON rows;
- warning/error persistence where warnings remain non-blocking;
- a single end-to-end staging entry point.

## Scope

Production code and tests contain no political-domain attributes or mappings. The framework is intentionally generic.

## Commands

```bash
npm run typecheck
npm test
npm run test:integration
npm run verify
```

The test suite is dependency-free and uses Node's built-in test runner. Integration tests use a transactional in-memory `Queryable` harness to validate orchestration, SQL call sequencing, warning/error semantics, and rollback behavior.

## PostgreSQL gate

The included migration is PostgreSQL-compatible SQL:

```sql
alter table import_stage_row
  add column if not exists raw_source_row jsonb;
```

This execution environment did not provide PostgreSQL or Docker, so the migration and SQL statements have not been executed against a live PostgreSQL server here. Live PostgreSQL execution remains a deployment/integration gate.

## Live PostgreSQL verification

A separate live gate now exercises the migration and the actual persistence function against PostgreSQL using a dedicated `pg` client connection. It checks:

- migration execution from a pre-migration `import_stage_row` table;
- `raw_source_row` is nullable `jsonb`;
- canonical and raw JSON remain distinct;
- warnings remain non-blocking and rows become `VALID`;
- blocking errors make rows `INVALID` and the batch `FAILED`;
- an injected database failure rolls back the batch status and all staged writes.

Run it where PostgreSQL is available:

```bash
npm install --no-save pg@8
PGHOST=127.0.0.1 \
PGPORT=5432 \
PGUSER=postgres \
PGPASSWORD=postgres \
PGDATABASE=postgres \
npm run test:postgres
```

The included `.github/workflows/postgres-integration.yml` provisions PostgreSQL 18 as a disposable service and runs both the dependency-free suite and this live gate.
