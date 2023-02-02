const { getQueryStatus } = require("./lib/cluster");
const { knex } = require("./lib/knex");
const logger = require("./lib/logger");
const stats = require("./lib/stats");
const { QUERY_STATUS } = require("./lib/query");

const BABYSITTER_DELAY_MS = process.env.BABYSITTER_DELAY_MS
  ? parseInt(process.env.BABYSITTER_DELAY_MS)
  : 3000;

// List of statuses to ignore checking. These are either pending or
// finished queries and Trino may not have data for them anymore
const IGNORED_STATUSES = [
  QUERY_STATUS.AWAITING_SCHEDULING,
  QUERY_STATUS.FAILED,
  QUERY_STATUS.FINISHED,
  QUERY_STATUS.LOST,
].join(",");

async function babysitQueries() {
  const startTime = new Date();

  const currentQueries = await knex.raw(
    `select * from query where not status ilike any('{${IGNORED_STATUSES}}')`
  );

  stats.increment("queries_waiting_updating", currentQueries.rows.length);
  for (const query of currentQueries.rows) {
    try {
      const status = await getQueryStatus(
        query.cluster_id,
        query.cluster_query_id
      );

      // Set new status - null means the query was lost
      const newStatus = status?.state || QUERY_STATUS.LOST;
      await knex("query")
        .where("id", query.id)
        .update({ status: newStatus, updated_at: new Date() });

      stats.increment("query_updated", [
        `previous_status:${query.status}`,
        `current_status:${newStatus}`,
      ]);
    } catch (err) {
      logger.error("Error checking query status", err);
    }
  }

  stats.timing("babysitter.timing", new Date() - startTime);
}

async function runBabysitAndReschedule() {
  try {
    await babysitQueries();
  } catch (err) {
    logger.error("Error babysitting", err);
  }

  // Reschdule task for the future
  setTimeout(runBabysitAndReschedule, BABYSITTER_DELAY_MS);
}

// Kick off initial babysit task
if (BABYSITTER_DELAY_MS) {
  logger.info(
    `Scheduling query babysitter to run every ${BABYSITTER_DELAY_MS}ms`
  );
  setTimeout(runBabysitAndReschedule, BABYSITTER_DELAY_MS);
}
