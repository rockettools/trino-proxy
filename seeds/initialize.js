const uuidv4 = require("uuid").v4;
const CLUSTER_STATUS = {
  ENABLED: "ENABLED",
  DISABLED: "DISABLED",
};

const now = new Date();

/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.seed = async function seed(knex) {
  // Clear out any existing users and re-insert test users
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
    name: "blue",
    password: "{}", // no password
    parsers: {
      user: "-- Username: *(.*)",
      tags: "-- Tags: *(.*)",
    },
    tags: "{}",
    options: {
      clusterTags: ["blue"],
    },
    updated_at: now,
    created_at: now,
  });

  // User with green cluster targeted in options
  await knex("user").insert({
    id: uuidv4(),
    name: "green",
    password: "{}", // no password
    parsers: {
      user: "-- Username: *(.*)",
      tags: "-- Tags: *(.*)",
    },
    tags: "{}",
    options: {
      clusterTags: ["green"],
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
      clusterTags: ["blue", "green"],
    },
    updated_at: now,
    created_at: now,
  });

  // Clear out any existing clusters and re-insert test clusters
  await knex("cluster").del();
  await knex("cluster").insert({
    id: uuidv4(),
    name: "trino1",
    url: "http://trino1:8080",
    status: CLUSTER_STATUS.ENABLED,
    tags: ["blue"],
    updated_at: now,
    created_at: now,
  });
  await knex("cluster").insert({
    id: uuidv4(),
    name: "trino2",
    url: "http://trino2:8080",
    status: CLUSTER_STATUS.ENABLED,
    tags: ["green"],
    updated_at: now,
    created_at: now,
  });
  await knex("cluster").insert({
    id: uuidv4(),
    name: "trino3",
    url: "http://trino3:8080",
    status: CLUSTER_STATUS.ENABLED,
    tags: [],
    updated_at: now,
    created_at: now,
  });

  // Clear out any existing queries
  await knex("query").del();
};
