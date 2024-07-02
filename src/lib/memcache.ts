import { LRUCache } from "lru-cache";
import { QueryUser, Trace } from "../types/models";

/**
 * This cache stores mappings of `x-trino-trace-token` headers
 * to the assumed user of the initial query. This is used for
 * clients that authenticate on the initial request using headers
 * and pass the trace token for future requests.
 *
 * There is no TTL on this cache as some queries are long-running
 * and we need to keep the mapping around for a while.
 */
export const traceCache = new LRUCache<string, Trace>({
  max: process.env.TRACE_CACHE_SIZE
    ? parseInt(process.env.TRACE_CACHE_SIZE)
    : 1000,
});

/**
 * This cache stores the `user` data from Postgres by ID, allowing
 * for quicker user lookup when determining which cluster to
 * route a query to.
 *
 * There is a short TTL on this cache to ensure that updates to the
 * user's cluster tags are fetched quickly by the service.
 */
export const userCache = new LRUCache<string, QueryUser>({
  max: process.env.USER_CACHE_SIZE
    ? parseInt(process.env.USER_CACHE_SIZE)
    : 1000,
  ttl: process.env.USER_CACHE_TTL_MS
    ? parseInt(process.env.USER_CACHE_TTL_MS)
    : 1000 * 60 * 3, // 3min
});
