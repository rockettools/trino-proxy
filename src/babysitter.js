const BPromise = require("bluebird");

const { getQueryStatus } = require("./lib/cluster");
const { knex } = require("./lib/knex");
const logger = require("./lib/logger");
const { QUERY_STATUS } = require("./lib/query");

const BABYSITTER_DISABLED = process.env.BABYSITTER_DISABLED === "true";
const BABYSITTER_DELAY = process.env.BABYSITTER_DELAY
  ? parseInt(process.env.BABYSITTER_DELAY)
  : 3000;

const COMPLETED_STATUSES = [
  QUERY_STATUS.LOST,
  QUERY_STATUS.FINISHED,
  QUERY_STATUS.FAILED,
].join(",");

async function babysitQueries() {
  const currentQueries = await knex.raw(
    `select * from query where not status ilike any('{${COMPLETED_STATUSES}}')`
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
          await knex("query")
            .where("id", query.id)
            .update({ status: QUERY_STATUS.LOST });
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

async function runBabysitAndReschedule() {
  try {
    await babysitQueries();
  } catch (err) {
    logger.error("Error babysitting", err);
  }

  // Reschdule task for the future
  setTimeout(runBabysitAndReschedule, BABYSITTER_DELAY);
}

// Kick off initial babysit task
if (!BABYSITTER_DISABLED) {
  logger.info(`Scheduling query babysitter to run every ${BABYSITTER_DELAY}ms`);
  runBabysitAndReschedule();
}
