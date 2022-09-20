const LRU = require("lru-cache");
module.exports = new LRU({
  max: process.env.TRACE_CACHE_SIZE
    ? parseInt(process.env.TRACE_CACHE_SIZE)
    : 100,
});
