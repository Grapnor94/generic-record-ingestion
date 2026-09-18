export type PostgresQueryResult<
  Row extends Record<string, unknown> = Record<string, unknown>,
> = {
  rowCount: number | null;
  rows: Row[];
};

export type PostgresQueryable = {
  query<Row extends Record<string, unknown> = Record<string, unknown>>(
    text: string,
    values?: readonly unknown[],
  ): Promise<PostgresQueryResult<Row>>;
};

export type PostgresPoolClient = PostgresQueryable & {
  release(): void;
};

export type PostgresDatabase =
  | { kind: "CLIENT"; client: PostgresQueryable }
  | { kind: "POOL"; pool: { connect(): Promise<PostgresPoolClient> } };

export async function withDedicatedConnection<Value>(
  database: PostgresDatabase,
  work: (client: PostgresQueryable) => Promise<Value>,
): Promise<Value> {
  if (database.kind === "CLIENT") {
    return work(database.client);
  }

  const client = await database.pool.connect();
  let hasPrimaryError = false;
  let primaryError: unknown;
  try {
    return await work(client);
  } catch (error) {
    hasPrimaryError = true;
    primaryError = error;
    throw error;
  } finally {
    try {
      client.release();
    } catch (cleanupError) {
      if (hasPrimaryError) {
        retainCleanupError(primaryError, cleanupError);
      } else {
        throw cleanupError;
      }
    }
  }
}

function retainCleanupError(primary: unknown, cleanup: unknown): void {
  if (
    (typeof primary !== "object" || primary === null)
    && typeof primary !== "function"
  ) {
    return;
  }

  try {
    const target = primary as { cleanupErrors?: unknown[] };
    const cleanupErrors = Array.isArray(target.cleanupErrors)
      ? [...target.cleanupErrors, cleanup]
      : [cleanup];
    Object.defineProperty(target, "cleanupErrors", {
      configurable: true,
      enumerable: false,
      value: cleanupErrors,
      writable: true,
    });
  } catch {
    // Cleanup annotation must never replace the primary operation error.
  }
}

export async function withTransaction<Value>(
  client: PostgresQueryable,
  work: (transaction: PostgresQueryable) => Promise<Value>,
): Promise<Value> {
  await client.query("begin");
  try {
    const value = await work(client);
    await client.query("commit");
    return value;
  } catch (error) {
    try {
      await client.query("rollback");
    } catch (cleanupError) {
      retainCleanupError(error, cleanupError);
    }
    throw error;
  }
}
