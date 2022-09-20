const StatsD = require("hot-shots");
module.exports = new StatsD({
  prefix: process.env.DD_PREFIX || "",
  port: 8020,
  globalTags: { env: process.env.NODE_ENV },
});
