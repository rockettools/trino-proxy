const { client } = require("./redis");
const { knex } = require("./knex");

exports.saveQueryIdMapping = async function saveQueryIdMapping(
  queryId,
  newQueryId
) {
  await knex("query")
    .where({ id: newQueryId })
    .update({ cluster_query_id: queryId });
  await client.set(newQueryId, queryId, {
    EX: 60 * 60,
  });
};

exports.getQueryById = async function getQueryIdMapping(newQueryId) {
  return knex("query").where({ id: newQueryId }).first();
};
