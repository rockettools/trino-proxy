/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.up = function (knex) {
  return knex.schema.alterTable("query", (table) => {
    table.specificType("trino_request_headers", "jsonb");
  });
};

/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.down = function (knex) {
  return knex.schema.alterTable("query", (table) => {
    table.dropColumn("trino_request_headers");
  });
};
