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
    options: {
      clusterTags: ["shopping"]
    },
    updated_at: now,
    created_at: now,
  });

  // User with email cluster targeted in options
  await knex("user").insert({
    id: uuidv4(),
    name: "email",
    password: "{}", // no password
    parsers: {
      user: "-- Username: *(.*)",
      tags: "-- Tags: *(.*)",
    },
    tags: "{}",
    options: {
      clusterTags: ["email"]
    },
    updated_at: now,
    created_at: now,
  });

  // User with all clusters targeted in options
    await knex("user").insert({
      id: uuidv4(),
      name: "all",
      password: "{}", // no password
      parsers: {
        user: "-- Username: *(.*)",
        tags: "-- Tags: *(.*)",
      },
      tags: "{}",
      options: {
        clusterTags: ["email", "shopping"]
      },
      updated_at: now,
      created_at: now,
    });

  await knex("cluster").del();
  await knex("cluster").insert({
    id: uuidv4(),
    name: "trino",
    url: "http://trino:8080",
    status: CLUSTER_STATUS.ENABLED,
    tags: ["shopping"],
    updated_at: now,
    created_at: now,
  });
  await knex("cluster").insert({
      id: uuidv4(),
      name: "trino2",
      url: "http://trino2:8080",
      status: CLUSTER_STATUS.ENABLED,
      tags: ["shopping", "email"],
      updated_at: now,
      created_at: now,
    });
  await knex("cluster").insert({
        id: uuidv4(),
        name: "trino3",
        url: "http://trino3:8080",
        status: CLUSTER_STATUS.ENABLED,
        tags: ["email"],
        updated_at: now,
        created_at: now,
      });
};
