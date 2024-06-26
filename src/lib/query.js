const { knex } = require("./knex");
const { traceCache } = require("./memcache");
const logger = require("./logger");
const stats = require("./stats");

const QUERY_STATUS = {
  // Trino Proxy states
  AWAITING_SCHEDULING: "AWAITING_SCHEDULING",
  CANCELLED: "CANCELLED",
  RESULT_SET_ROW_LIMIT: "RESULT_SET_ROW_LIMIT",
  MAX_DOWNLOAD_BYTES_LIMIT: "MAX_DOWNLOAD_BYTES_LIMIT",
  NO_VALID_CLUSTERS: "NO_VALID_CLUSTERS",
  // Trino states
  BLOCKED: "BLOCKED",
  FAILED: "FAILED",
  FINISHED: "FINISHED",
  FINISHING: "FINISHING",
  LOST: "LOST",
  PLANNING: "PLANNING",
  QUEUED: "QUEUED",
  RUNNING: "RUNNING",
  STARTING: "STARTING",
};

async function getQueryById(newQueryId) {
  return knex("query").where({ id: newQueryId }).first();
}

async function getFirstQueryByTraceId(traceId) {
  return knex("query")
    .where({ trace_id: traceId })
    .orderBy("created_at", "asc")
    .first();
}

async function getQueryHeaderInfo(traceId) {
  if (!traceId) {
    return null;
  }

  const cachedHeaderData = traceCache.get(traceId);
  if (cachedHeaderData) {
    return cachedHeaderData;
  }

  const firstQueryInTrace = await getFirstQueryByTraceId(traceId);
  if (firstQueryInTrace) {
    return {
      user: firstQueryInTrace.assumed_user || firstQueryInTrace.user,
      tags: firstQueryInTrace.tags || [],
    };
  }

  return null;
}

async function updateQuery(queryId, data = {}) {
  try {
    await knex("query").where({ id: queryId }).update(data);
  } catch (err) {
    logger.error("Error updating query status", err, { queryId });
  }

  logger.debug("Updated query", { queryId, data });
  stats.increment("query_updated", [`status:${data.status}`]);
}

function parseFirstQueryHeader(query, parsers = {}) {
  const parsedInfo = {
    user: null,
    tags: [],
  };

  if (parsers?.user) {
    const parsedUser = new RegExp(parsers.user).exec(query);
    if (parsedUser) {
      parsedInfo.user = parsedUser[1];
    }
  }

  if (parsers?.tags) {
    const parsedTags = new RegExp(parsers.tags).exec(query);
    if (parsedTags && parsedTags[1]) {
      const tags = parsedTags[1].split(",");
      parsedInfo.tags.push(...tags);
    }
  }

  return parsedInfo;
}

module.exports = {
  getQueryById,
  getQueryHeaderInfo,
  parseFirstQueryHeader,
  QUERY_STATUS,
  updateQuery,
};
