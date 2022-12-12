const { knex } = require("./knex");
const cache = require("./memcache");

const QUERY_STATUS = {
  AWAITING_SCHEDULING: "awaiting_scheduling",
  FAILED: "failed",
  FINISHED: "finished",
  LOST: "lost",
  QUEUED: "queued",
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

module.exports = {
  getAssumedUserForTrace,
  getQueryById,
  QUERY_STATUS,
};
