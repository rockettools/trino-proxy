const { LRUCache } = require("lru-cache");

const maxCacheSize = process.env.TRACE_CACHE_SIZE
  ? parseInt(process.env.TRACE_CACHE_SIZE)
  : 1000;

module.exports = new LRUCache({
  max: maxCacheSize,
});
