import StatsD from "hot-shots";

const host = process.env.STATSD_HOST || "localhost";
const port = process.env.STATSD_PORT ? parseInt(process.env.STATSD_PORT) : 8125;
const prefix = process.env.STATSD_PREFIX || "trino_proxy";
const env = process.env.NODE_ENV || "production";

export default new StatsD({
  host,
  port,
  prefix: prefix + ".",
  globalTags: { env },
});
