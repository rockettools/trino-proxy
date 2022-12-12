const { knex } = require("./knex");
const cache = require("./memcache");

const QUERY_STATUS = {
  AWAITING_SCHEDULING: "awaiting_scheduling",
  FAILED: "failed",
  FINISHED: "finished",
  LOST: "lost",
  QUEUED: "queued",
};

async function saveQueryIdMapping(queryId, newQueryId) {
  await knex("query")
    .where({ id: newQueryId })
    .update({ cluster_query_id: queryId });
}

async function getQueryIdMapping(newQueryId) {
  return knex("query").where({ id: newQueryId }).first();
}

async function getQueryByTraceId(traceId) {
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

  const previousQuery = await getQueryByTraceId(traceId);
  if (previousQuery) {
    return previousQuery.assumed_user || previousQuery.user;
  }
  return null;
}

module.exports = {
  getAssumedUserForTrace,
  getQueryByTraceId,
  getQueryIdMapping,
  saveQueryIdMapping,
  QUERY_STATUS,
};
