const BPromise = require("bluebird");

const { getQueryStatus } = require("./lib/cluster");
const { knex } = require("./lib/knex");
const logger = require("./lib/logger");
const { QUERY_STATUS } = require("./lib/query");
const stats = require("./lib/stats");

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
      try {
        if (!query.cluster_id) {
          logger.error("Missing cluster id for query", { query: query.id });
          return;
        }

        const status = await getQueryStatus(
          query.cluster_id,
          query.cluster_query_id
        );

        // Set new status - null means the query was lost
        const newStatus =
          status && status.state ? status.state : QUERY_STATUS.LOST;
        await knex("query")
          .where("id", query.id)
          .update({ status: newStatus, updated_at: new Date() });

        stats.increment("update_status.complete", [`status:${newStatus}`]);
      } catch (err) {
        logger.error("Error updating old queries", err);
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
  runBabysitAndReschedule();
}
