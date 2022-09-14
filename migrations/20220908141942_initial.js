/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.up = function (knex) {
  return knex.schema
    .createTable("query", function (table) {
      table.uuid("id").notNullable();
      table.string("status");
      table.string("body");
      table.string("cluster_id");
      table.string("cluster_query_id");
      table.string("trace_id");
      table.string("assumed_user");
      table.string("next_uri");

      table.index("id");

      table.index("status");
    })
    .createTable("cluster", function (table) {
      table.uuid("id").notNullable();
      table.string("name").notNullable();
      table.string("url").notNullable();
      table.string("status").notNullable();

      table.index("id");
    });
};

/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.down = function (knex) {
  return knex.schema.dropTable("query");
};
