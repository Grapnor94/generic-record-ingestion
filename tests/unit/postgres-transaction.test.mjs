import test from "node:test";
import assert from "node:assert/strict";
import {
  withDedicatedConnection,
  withTransaction,
} from "../../dist/db/postgres.js";

class RecordingClient {
  constructor(failures = {}, releaseFailure = null) {
    this.failures = failures;
    this.releaseFailure = releaseFailure;
    this.sql = [];
    this.receivers = [];
    this.releaseCalls = 0;
  }

  async query(text) {
    const normalized = text.trim().toLowerCase();
    this.sql.push(normalized);
    this.receivers.push(this);
    if (this.failures[normalized]) {
      throw this.failures[normalized];
    }
    return { rowCount: null, rows: [] };
  }

  release() {
    this.releaseCalls += 1;
    if (this.releaseFailure) {
      throw this.releaseFailure;
    }
  }
}

function recordingPool(client) {
  return {
    connectCalls: 0,
    async connect() {
      this.connectCalls += 1;
      return client;
    },
    async query() {
      throw new Error("pool.query must not be used for dedicated work");
    },
  };
}

test("pool transactions use one acquired client and always release it", async () => {
  const acquired = new RecordingClient();
  const pool = recordingPool(acquired);

  const value = await withDedicatedConnection(
    { kind: "POOL", pool },
    (client) => withTransaction(client, async (tx) => {
      await tx.query("work");
      return 42;
    }),
  );

  assert.equal(value, 42);
  assert.equal(pool.connectCalls, 1);
  assert.deepEqual(acquired.sql, ["begin", "work", "commit"]);
  assert.deepEqual(acquired.receivers, [acquired, acquired, acquired]);
  assert.equal(acquired.releaseCalls, 1);
});

test("caller-owned clients pass through untouched and are not released", async () => {
  const client = new RecordingClient();
  let received;

  const value = await withDedicatedConnection(
    { kind: "CLIENT", client },
    async (dedicated) => {
      received = dedicated;
      return withTransaction(dedicated, async (tx) => {
        assert.equal(tx, client);
        return "ok";
      });
    },
  );

  assert.equal(value, "ok");
  assert.equal(received, client);
  assert.deepEqual(client.sql, ["begin", "commit"]);
  assert.equal(client.releaseCalls, 0);
});

test("work failures roll back and preserve the primary error object", async () => {
  const client = new RecordingClient();
  const primary = new Error("work failed");

  await assert.rejects(
    () => withTransaction(client, async () => {
      throw primary;
    }),
    (error) => error === primary,
  );

  assert.deepEqual(client.sql, ["begin", "rollback"]);
});

test("commit failures roll back and preserve the commit error object", async () => {
  const primary = new Error("commit failed");
  const client = new RecordingClient({ commit: primary });

  await assert.rejects(
    () => withTransaction(client, async (tx) => {
      await tx.query("work");
    }),
    (error) => error === primary,
  );

  assert.deepEqual(client.sql, ["begin", "work", "commit", "rollback"]);
});

test("rollback failures are retained as non-enumerable cleanup errors", async () => {
  const primary = new Error("work failed");
  const cleanup = new Error("rollback failed");
  const client = new RecordingClient({ rollback: cleanup });

  await assert.rejects(
    () => withTransaction(client, async () => {
      throw primary;
    }),
    (error) => {
      assert.equal(error, primary);
      assert.deepEqual(error.cleanupErrors, [cleanup]);
      assert.equal(Object.prototype.propertyIsEnumerable.call(error, "cleanupErrors"), false);
      return true;
    },
  );

  assert.deepEqual(client.sql, ["begin", "rollback"]);
});

test("rollback and release failures stay secondary to the operation error", async () => {
  const primary = new Error("work failed");
  const rollbackFailure = new Error("rollback failed");
  const releaseFailure = new Error("release failed");
  const acquired = new RecordingClient(
    { rollback: rollbackFailure },
    releaseFailure,
  );
  const pool = recordingPool(acquired);

  await assert.rejects(
    () => withDedicatedConnection(
      { kind: "POOL", pool },
      (client) => withTransaction(client, async () => {
        throw primary;
      }),
    ),
    (error) => {
      assert.equal(error, primary);
      assert.deepEqual(error.cleanupErrors, [rollbackFailure, releaseFailure]);
      assert.equal(Object.prototype.propertyIsEnumerable.call(error, "cleanupErrors"), false);
      return true;
    },
  );

  assert.deepEqual(acquired.sql, ["begin", "rollback"]);
  assert.equal(acquired.releaseCalls, 1);
});

test("frozen primary errors survive rollback annotation failure", async () => {
  const primary = Object.freeze(new Error("work failed"));
  const rollbackFailure = new Error("rollback failed");
  const client = new RecordingClient({ rollback: rollbackFailure });

  await assert.rejects(
    () => withTransaction(client, async () => {
      throw primary;
    }),
    (error) => error === primary,
  );

  assert.deepEqual(client.sql, ["begin", "rollback"]);
});

test("begin failures do not attempt rollback", async () => {
  const primary = new Error("begin failed");
  const client = new RecordingClient({ begin: primary });
  let workCalls = 0;

  await assert.rejects(
    () => withTransaction(client, async () => {
      workCalls += 1;
    }),
    (error) => error === primary,
  );

  assert.equal(workCalls, 0);
  assert.deepEqual(client.sql, ["begin"]);
});

test("pool clients release once after every acquired transaction failure path", async (t) => {
  const paths = [
    {
      name: "begin failure",
      client: () => new RecordingClient({ begin: new Error("begin failed") }),
      work: async () => {},
    },
    {
      name: "work failure",
      client: () => new RecordingClient(),
      work: async () => { throw new Error("work failed"); },
    },
    {
      name: "commit failure",
      client: () => new RecordingClient({ commit: new Error("commit failed") }),
      work: async () => {},
    },
    {
      name: "rollback failure",
      client: () => new RecordingClient({ rollback: new Error("rollback failed") }),
      work: async () => { throw new Error("work failed"); },
    },
  ];

  for (const path of paths) {
    await t.test(path.name, async () => {
      const acquired = path.client();
      const pool = recordingPool(acquired);
      await assert.rejects(
        () => withDedicatedConnection(
          { kind: "POOL", pool },
          (client) => withTransaction(client, path.work),
        ),
      );
      assert.equal(pool.connectCalls, 1);
      assert.equal(acquired.releaseCalls, 1);
    });
  }
});
