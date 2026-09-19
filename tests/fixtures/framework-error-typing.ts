import { FrameworkError } from "../../dist/errors.js";

const error = new FrameworkError(
  "IMPORT_NOT_FOUND",
  "Import attempt was not found.",
);

const message: string = error.message;
void message;
