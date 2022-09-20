/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.up = function (knex) {
  return knex.schema
    .createTable("query", function (table) {
      table.uuid("id").notNullable();
      table.string("status");
      table.text("body");
      table.string("cluster_id");
      table.string("cluster_query_id");
      table.string("trace_id");
      table.string("user");
      table.string("assumed_user");
      table.string("next_uri");
      table.timestamp("updated_at");
      table.timestamp("created_at");

      table.index("id");

      table.index("status");
    })
    .createTable("cluster", function (table) {
      table.uuid("id").notNullable().unique();
      table.string("name").notNullable().unique();
      table.string("url").notNullable();
      table.string("status").notNullable();

      table.timestamp("updated_at");
      table.timestamp("created_at");

      table.index("id");
    })
    .createTable("user", function (table) {
      table.uuid("id").notNullable().unique();
      table.string("name").notNullable().unique();
      table.string("role");
      table.specificType("password", "varchar[]"); // array for downtimeless password rotation
      table.specificType("parsers", "jsonb");

      table.timestamp("updated_at");
      table.timestamp("created_at");

      table.index("id");
      table.index("name");
    });
};

/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.down = function (knex) {
  return knex.schema.dropTable("query");
};
