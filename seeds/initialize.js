const uuidv4 = require("uuid").v4;
const now = new Date();

/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.seed = async function (knex) {
  await knex("user").del();
  await knex("user").insert({
    id: uuidv4(),
    name: "admin",
    password: "{}", // no password
    parsers: null,
    updated_at: now,
    created_at: now,
  });

  await knex("cluster").del();
  await knex("cluster").insert({
    id: uuidv4(),
    name: "trino",
    url: "http://trino:8080",
    status: "enabled",
    updated_at: now,
    created_at: now,
  });
};
