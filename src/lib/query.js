const { knex } = require("./knex");
const cache = require("./memcache");

exports.saveQueryIdMapping = async function saveQueryIdMapping(
  queryId,
  newQueryId
) {
  await knex("query")
    .where({ id: newQueryId })
    .update({ cluster_query_id: queryId });
};

exports.getQueryById = async function getQueryIdMapping(newQueryId) {
  return knex("query").where({ id: newQueryId }).first();
};

exports.getFirstQueryByTraceId = async function getQueryByTraceId(traceId) {
  return knex("query")
    .where({ trace_id: traceId })
    .orderBy("created_at", "asc")
    .first();
};

exports.getAssumedUserForTrace = async function (traceId) {
  // trinoTraceToken = req.headers["x-trino-trace-token"];
  const cachedUser = await cache.get(traceId);
  if (cachedUser) {
    return cachedUser;
  }

  const previousQuery = await exports.getFirstQueryByTraceId(traceId);
  if (previousQuery) {
    return previousQuery.assumed_user || previousQuery.user;
  }
  return null;
};
