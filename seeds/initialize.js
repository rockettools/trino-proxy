const uuidv4 = require("uuid").v4;
const { CLUSTER_STATUS } = require("../src/lib/trino");

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
    tags: "{admin}",
    updated_at: now,
    created_at: now,
  });

  // Main user with headers
  await knex("user").insert({
    id: uuidv4(),
    name: "main",
    password: "{}", // no password
    parsers: {
      user: "-- Username: *(.*)",
      tags: "-- Tags: *(.*)",
    },
    tags: "{}",
    updated_at: now,
    created_at: now,
  });

  await knex("cluster").del();
  await knex("cluster").insert({
    id: uuidv4(),
    name: "trino",
    url: "http://trino:8080",
    status: CLUSTER_STATUS.ENABLED,
    updated_at: now,
    created_at: now,
  });
};
