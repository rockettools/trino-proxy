const { knex } = require("./knex");
const cache = require("./memcache");
const logger = require("./logger");
const stats = require("./stats");

const QUERY_STATUS = {
  AWAITING_SCHEDULING: "AWAITING_SCHEDULING",
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

async function getAssumedUserForTrace(traceId) {
  const cachedUser = await cache.get(traceId);
  if (cachedUser) {
    return cachedUser;
  }

  const previousQuery = await getFirstQueryByTraceId(traceId);
  if (previousQuery) {
    return previousQuery.assumed_user || previousQuery.user;
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

module.exports = {
  getAssumedUserForTrace,
  getQueryById,
  updateQuery,
  QUERY_STATUS,
};
