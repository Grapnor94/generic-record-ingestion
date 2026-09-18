import test from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { FrameworkError } from "../../dist/errors.js";

const require = createRequire(import.meta.url);
const root = fileURLToPath(new URL("../../", import.meta.url));

test("FrameworkError accepts and retains a descriptive message", () => {
  const cause = new Error("database unavailable");
  const details = { importId: "IMP-404" };
  const error = new FrameworkError(
    "IMPORT_NOT_FOUND",
    "Import attempt IMP-404 was not found.",
    { cause, details },
  );

  assert.equal(error.code, "IMPORT_NOT_FOUND");
  assert.equal(error.message, "Import attempt IMP-404 was not found.");
  assert.equal(error.cause, cause);
  assert.equal(error.details, details);
});

test("FrameworkError declaration accepts a descriptive string", () => {
  const tsc = require.resolve("typescript/bin/tsc");
  const result = spawnSync(
    process.execPath,
    [
      tsc,
      "--noEmit",
      "--strict",
      "--target", "ES2022",
      "--module", "NodeNext",
      "--moduleResolution", "NodeNext",
      "tests/fixtures/framework-error-typing.ts",
    ],
    { cwd: root, encoding: "utf8" },
  );

  assert.ifError(result.error);
  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
});
