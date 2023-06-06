/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.up = function (knex) {
  return knex.schema
    .alterTable("query", (table) => {
      table.specificType("total_rows", "integer");
    })
    .alterTable("query", (table) => {
      table.string("query");
      table.specificType("total_bytes", "bigint");
    });
};

/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.down = function (knex) {
  return knex.schema
    .alterTable("query", (table) => {
      table.dropColumn("total_rows");
    })
    .alterTable("query", (table) => {
      table.dropColumn("total_bytes");
    });
};
