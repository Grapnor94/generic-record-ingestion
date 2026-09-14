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
npm run db:migrate
```

The test suite is dependency-free and uses Node's built-in test runner. Integration tests use a transactional in-memory `Queryable` harness to validate orchestration, SQL call sequencing, warning/error semantics, and rollback behavior.

## Database bootstrap and migrations

The package owns these PostgreSQL tables:

- `import_batch`
- `import_stage_row`
- `import_issue`
- `schema_migration`

Apply all pending migrations with:

```bash
npm run db:migrate
```

The command uses `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, and `PGDATABASE`. Migration files live in `db/migrations`, are applied in lexical filename order, and are forward-only. Applied filenames are recorded in `schema_migration`; rerunning the command skips migrations already present in the ledger.

Committed migrations are immutable. Introduce schema changes with a new migration file rather than editing an already-published migration.

## Live PostgreSQL verification

The live gate starts from an empty isolated PostgreSQL schema, applies the repository migrations with the same migration runner used by `npm run db:migrate`, and then exercises the actual persistence function. It checks:

- clean-schema bootstrap creates all four package-owned tables;
- migrations apply in lexical order and are recorded in `schema_migration`;
- rerunning migrations is idempotent and skips applied filenames;
- `raw_source_row` is nullable `jsonb`;
- canonical and raw JSON remain distinct;
- warnings remain non-blocking and rows become `VALID`;
- blocking errors make rows `INVALID` and the batch `FAILED`;
- an injected database failure rolls back the batch status and all staged writes.

Run it where PostgreSQL is available:

```bash
PGHOST=127.0.0.1 \
PGPORT=5432 \
PGUSER=postgres \
PGPASSWORD=postgres \
PGDATABASE=postgres \
npm run test:postgres
```

The included `.github/workflows/postgres-integration.yml` provisions PostgreSQL 18 as a disposable service and runs both the normal verification suite and this live gate.
