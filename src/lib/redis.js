const { createClient } = require("redis");
const logger = require("./logger");
const { REDIS_URL } = process.env;

exports.client = createClient({ url: REDIS_URL });
exports.client.on("error", (err) =>
  logger.error("Redis client error", { err, url: REDIS_URL })
);

exports.client.connect();
