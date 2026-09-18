import { readFile, readdir } from "node:fs/promises";
import { createHash } from "node:crypto";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  retainCleanupError,
  withDedicatedConnection,
  withTransaction,
  type PostgresDatabase,
  type PostgresQueryable,
} from "./postgres.js";
import { FrameworkError } from "../errors.js";

const MIGRATION_FILENAME = /^[0-9]{4}_.+\.sql$/;
const MODULE_DIR = dirname(fileURLToPath(import.meta.url));
const DEFAULT_MIGRATIONS_DIR = resolve(MODULE_DIR, "../../db/migrations");
// generic-record-ingestion migration lock: a fixed pair of signed int32 keys.
const MIGRATION_LOCK_NAMESPACE = 0x475249;
const MIGRATION_LOCK_ID = 0x4d494752;

export type MigrationResult = {
  applied: string[];
  verified: string[];
  legacyUnverified: string[];
};

export type MigrationFile = {
  filename: string;
  absolutePath: string;
};

export async function discoverMigrations(
  migrationsDir: string,
): Promise<MigrationFile[]> {
  const entries = await readdir(migrationsDir, { withFileTypes: true });
  const sqlFiles = entries
    .filter((entry) => entry.isFile() && entry.name.endsWith(".sql"))
    .map((entry) => entry.name);

  for (const filename of sqlFiles) {
    if (!MIGRATION_FILENAME.test(filename)) {
      throw new Error(`Invalid migration filename: ${filename}`);
    }
  }

  return sqlFiles
    .sort((a, b) => a.localeCompare(b))
    .map((filename) => ({
      filename,
      absolutePath: resolve(migrationsDir, filename),
    }));
}

export async function runMigrations(
  database: PostgresDatabase,
  options: { migrationsDir?: string } = {},
): Promise<MigrationResult> {
  return withDedicatedConnection(database, async (client) => {
    const lockKeys = [MIGRATION_LOCK_NAMESPACE, MIGRATION_LOCK_ID];
    await client.query("select pg_advisory_lock($1, $2)", lockKeys);
    let hasPrimaryError = false;
    let primaryError: unknown;
    try {
      return await runMigrationsOnConnection(client, options);
    } catch (error) {
      hasPrimaryError = true;
      primaryError = error;
      throw error;
    } finally {
      try {
        await client.query("select pg_advisory_unlock($1, $2)", lockKeys);
      } catch (cleanupError) {
        if (hasPrimaryError) {
          retainCleanupError(primaryError, cleanupError);
        } else {
          throw cleanupError;
        }
      }
    }
  });
}

async function runMigrationsOnConnection(
  db: PostgresQueryable,
  options: { migrationsDir?: string },
): Promise<MigrationResult> {
  const migrationsDir = options.migrationsDir ?? DEFAULT_MIGRATIONS_DIR;

  await db.query(`
    create table if not exists schema_migration (
      filename text primary key,
      applied_at timestamptz not null default current_timestamp
    )
  `);
  // Upgrading the old ledger deliberately leaves historical checksums NULL.
  await db.query(`
    alter table schema_migration
      add column if not exists checksum_sha256 text
      constraint schema_migration_checksum_sha256_check
      check (checksum_sha256 is null or checksum_sha256 ~ '^[0-9a-f]{64}$')
  `);

  const migrationFiles = await discoverMigrations(migrationsDir);
  // Keep the bytes that were hashed so SQL execution uses the same snapshot.
  const migrations = await Promise.all(migrationFiles.map(async (migration) => {
    const bytes = await readFile(migration.absolutePath);
    return {
      ...migration,
      sql: bytes.toString("utf8"),
      checksum: createHash("sha256").update(bytes).digest("hex"),
    };
  }));
  const byFilename = new Map(migrations.map(migration => [migration.filename, migration]));
  const appliedRows = await db.query<{ filename: string; checksum_sha256: string | null }>(
    "select filename, checksum_sha256 from schema_migration order by filename",
  );
  const appliedSet = new Set(appliedRows.rows.map((row) => row.filename));

  const result: MigrationResult = { applied: [], verified: [], legacyUnverified: [] };

  // Validate every checksummed row before applying any pending migration.
  for (const row of appliedRows.rows) {
    if (row.checksum_sha256 === null) {
      result.legacyUnverified.push(row.filename);
      continue;
    }
    const actual = byFilename.get(row.filename)?.checksum ?? null;
    if (actual !== row.checksum_sha256) {
      throw new FrameworkError(
        "MIGRATION_CHECKSUM_MISMATCH",
        `Migration checksum mismatch: ${row.filename}`,
        { details: { filename: row.filename, expected: row.checksum_sha256, actual } },
      );
    }
    result.verified.push(row.filename);
  }

  for (const migration of migrations) {
    if (appliedSet.has(migration.filename)) continue;
    await withTransaction(db, async (transaction) => {
      await transaction.query(migration.sql);
      await transaction.query(
        "insert into schema_migration (filename, checksum_sha256) values ($1, $2)",
        [migration.filename, migration.checksum],
      );
    });
    result.applied.push(migration.filename);
  }

  return result;
}
