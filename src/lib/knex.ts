import Knex from "knex";

const { DB_URL, KNEX_POOL_MIN, KNEX_POOL_MAX, KNEX_CONNECTION_TIMEOUT } =
  process.env;

if (!DB_URL) {
  throw new Error("Database connection string required: DB_URL");
}

const poolMinimum = KNEX_POOL_MIN ? parseInt(KNEX_POOL_MIN) : 0;
const poolMaximum = KNEX_POOL_MAX ? parseInt(KNEX_POOL_MAX) : 10;
const connectionTimeout = KNEX_CONNECTION_TIMEOUT
  ? parseInt(KNEX_CONNECTION_TIMEOUT)
  : 10000;

export const knex = Knex({
  client: "pg",
  connection: DB_URL,
  /** It is recommended to set min to 0 so idle connections can be terminated */
  /** @see https://knexjs.org/guide/#pool */
  pool: {
    min: poolMinimum,
    max: poolMaximum,
  },
  /** How long knex waits before throwing a timeout error while acquiring a connection */
  /** @see https://knexjs.org/guide/#acquireconnectiontimeout */
  acquireConnectionTimeout: connectionTimeout,
});
