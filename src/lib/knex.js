const connectionString = process.env.DB_URL;
if (!connectionString) {
  throw new Error("Database connection string required: DB_URL");
}

exports.knex = require("knex")({
  client: "pg",
  connection: connectionString,
});
