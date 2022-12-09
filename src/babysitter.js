const BPromise = require("bluebird");

const { getQueryStatus } = require("./lib/cluster");
const { knex } = require("./lib/knex");
const logger = require("./lib/logger");

const BABYSIT_DELAY = process.env.BABYSIT_DELAY
  ? parseInt(process.env.BABYSIT_DELAY)
  : 3000;

async function babysit() {
  const currentQueries = await knex.raw(
    `select * from query where not status ilike any('{lost,finished,failed}')`
  );

  await BPromise.map(
    currentQueries.rows,
    async function (query) {
      if (query.cluster_id) {
        const status = await getQueryStatus(
          query.cluster_id,
          query.cluster_query_id
        );

        // if not found, mark as lost
        if (status === null) {
          await knex("query").where("id", query.id).update({ status: "lost" });
          return;
        }

        // TODO: Ideally we'd be updating a bunch of the other stats here
        await knex("query")
          .where("id", query.id)
          .update({ status: status.state });
      }
    },
    { concurrency: 1 }
  );
}

module.exports = async function () {
  try {
    await babysit();
  } catch (err) {
    logger.error("Error babysitting", err);
  }

  setTimeout(module.exports, BABYSIT_DELAY);
};
