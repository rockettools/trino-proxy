const StatsD = require("hot-shots");

const {
  STATSD_PREFIX = "trino_proxy",
  STATSD_PORT = "8125",
  STATSD_HOST = "localhost",
} = process.env;

module.exports = new StatsD({
  host: STATSD_HOST,
  port: STATSD_PORT,
  prefix: STATSD_PREFIX + ".",
  globalTags: { env: process.env.NODE_ENV },
});
