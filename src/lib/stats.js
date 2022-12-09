const StatsD = require("hot-shots");

const {
  STATS_PREFIX = "trino_proxy",
  STATS_PORT = "8125",
  STATS_HOST = "localhost",
} = process.env;

module.exports = new StatsD({
  host: STATS_HOST,
  port: STATS_PORT,
  prefix: STATS_PREFIX,
  globalTags: { env: process.env.NODE_ENV },
});
