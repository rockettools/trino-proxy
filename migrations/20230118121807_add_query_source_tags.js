/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.up = function (knex) {
  return knex.schema
    .alterTable("user", (table) => {
      table.specificType("tags", "varchar[]");
    })
    .alterTable("query", (table) => {
      table.string("source");
      table.specificType("tags", "varchar[]");
    });
};

/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.down = function (knex) {
  return knex.schema
    .alterTable("user", (table) => {
      table.dropColumn("tags");
    })
    .alterTable("query", (table) => {
      table.dropColumn("source");
      table.dropColumn("tags");
    });
};
