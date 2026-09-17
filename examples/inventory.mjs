import { prepareRecordStaging } from "generic-record-ingestion";

// These are already-parsed synthetic records, not a CSV parser or database importer.
const contract = {
  schemaVersion: "INVENTORY_EXAMPLE_V1",
  requiredHeaders: ["record_id", "item_name", "quantity", "warehouse"],
  optionalHeaders: ["legacy_code"],
};
const headers = [...contract.requiredHeaders, "legacy_code"];
const item = {
  record_id: " SKU-001 ", item_name: "  Widget  ", quantity: " 12 ",
  warehouse: " north ", legacy_code: "",
};

function prepare(rows) {
  return prepareRecordStaging({
    contract, headers, rows,
    transform: row => {
      const text = row.quantity.trim();
      const value = Number(text);
      return {
        itemName: row.item_name.trim(),
        quantity: /^\d+$/.test(text) && Number.isSafeInteger(value) ? value : null,
        warehouse: row.warehouse.trim().toUpperCase(),
      };
    },
    getRecordId: row => row.record_id.trim() || null,
    diagnose: (row, canonical) => {
      const diagnostics = [];
      if (canonical.quantity === null) {
        diagnostics.push({ code: "INVALID_QUANTITY", severity: "ERROR",
          fieldKey: "quantity", detail: "Quantity must be a nonnegative safe integer." });
      }
      if (row.legacy_code.trim()) {
        diagnostics.push({ code: "LEGACY_CODE", severity: "WARNING",
          fieldKey: "legacy_code", detail: "Legacy code retained in raw data for review." });
      }
      return diagnostics;
    },
  });
}

console.log(JSON.stringify({
  clean: prepare([item]),
  warning: prepare([{ ...item, legacy_code: " OLD-7 " }]),
  invalidQuantity: prepare([{ ...item, quantity: "-2" }]),
  duplicate: prepare([item, { ...item, item_name: "Second widget" }]),
}, null, 2));
