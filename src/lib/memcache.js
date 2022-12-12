const LRU = require("lru-cache");

const maxCacheSize = process.env.TRACE_CACHE_SIZE
  ? parseInt(process.env.TRACE_CACHE_SIZE)
  : 100;

module.exports = new LRU({
  max: maxCacheSize,
});
