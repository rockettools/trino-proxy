const connectionString = process.env.DB_URL;
if (!connectionString) {
  throw new Error("Database connection string required: DB_URL");
}

exports.knex = require("knex")({
  client: "pg",
  connection: connectionString,
  /** It is recommended to set min to 0 so idle connections can be terminated */
  /** @see https://knexjs.org/guide/#pool */
  pool: {
    min: 0,
    max: 10,
  },
  /** How long knex waits before throwing a timeout error while acquiring a connection */
  /** @see https://knexjs.org/guide/#acquireconnectiontimeout */
  acquireConnectionTimeout: 10000,
});
