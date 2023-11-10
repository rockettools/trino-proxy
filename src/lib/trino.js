const axios = require("axios").default;
const bluebird = require("bluebird");

const { knex } = require("../lib/knex");
const logger = require("../lib/logger");
const stats = require("../lib/stats");
const { QUERY_STATUS } = require("../lib/query");

const CLUSTER_STATUS = {
  ENABLED: "ENABLED",
  DISABLED: "DISABLED",
};

let schedulerRunning = false;
const SCHEDULER_DELAY_MS = 1000 * 15;
const MAX_QUERIES_QUEUED = 10;

/**
 * Checks that the Trino cluster is finished initializing.
 * The Trino http server starts before queries are ready to be queued, causing them to fail.
 * This function throws an error if the cluster is is not accessible or is initializing.
 *
 * TODO: improve this to cache the server status or prefetch it to minimize time between
 * trino-proxy accepting a new query and it being passed off to a Trino cluster.
 */
async function isClusterHealthy(clusterBaseUrl) {
  if (!clusterBaseUrl) {
    throw new Error("Missing clusterBaseUrl");
  }

  try {
    const infoResponse = await axios({
      url: `${clusterBaseUrl}/v1/info`,
      method: "get",
    });

    // Cluster is healthy if `starting` property is false
    const isHealthy = infoResponse.data && infoResponse.data.starting === false;
    return isHealthy;
  } catch (err) {
    logger.error("Error checking cluster health", err, {
      clusterBaseUrl,
    });
    return false;
  }
}

/**
 * Gets all available and healthy Trino clusters.
 */
async function getAvailableClusters() {
  const enabledClusters = await knex("cluster").where({
    status: CLUSTER_STATUS.ENABLED,
  });

  // Filter to only clusters that are healthy and ready for queries
  const availableClusters = await bluebird.filter(
    enabledClusters,
    async (cluster) => {
      const clusterHealthy = await isClusterHealthy(cluster.url);
      stats.increment("check_cluster_health", [
        `cluster:${cluster.name}`,
        `healthy:${clusterHealthy}`,
      ]);
      return clusterHealthy;
    },
  );

  stats.gauge("available_clusters", availableClusters.length);
  return availableClusters;
}

async function scheduleQueries() {
  if (schedulerRunning) return;
  const startTime = new Date();

  try {
    const queriesToSchedule = await knex("query")
      .where({ status: QUERY_STATUS.AWAITING_SCHEDULING })
      .count("*");

    // Increment number of pending queries for monitoring
    // If there are none, just early exit to prevent the extra db calls
    const numberQueriesPending = parseInt(queriesToSchedule[0].count) || 0;
    stats.gauge("queries_waiting_scheduling", numberQueriesPending);
    if (numberQueriesPending === 0) return;

    const availableClusters = await getAvailableClusters();
    if (availableClusters.length === 0) {
      logger.error("No healthy clusters available");
      return;
    }

    logger.debug("Scheduling pending queries", {
      queries: queriesToSchedule.length,
      availableClusters: availableClusters.length,
    });

    let currentClusterId = Math.floor(Math.random() * availableClusters.length);
    for (let idx = 0; idx <= MAX_QUERIES_QUEUED; idx++) {
      try {
        await knex.transaction(async (trx) => {
          // Select a single query that needs to be scheduled with a row-level lock
          // to prevent other schedulers on other instances from picking it up
          const query = await knex("query")
            .where({ status: QUERY_STATUS.AWAITING_SCHEDULING })
            .transacting(trx)
            .forUpdate()
            .skipLocked()
            .first();

          // If no query could be found, early exit
          if (!query) return;

          // Simple distribution of queries across all clusters
          // TODO: Could improve this based on given weights or cluster size
          const cluster = availableClusters[currentClusterId];
          currentClusterId = (currentClusterId + 1) % availableClusters.length;

          const user = query.assumed_user || query.user;
          const source = query.source || "trino-proxy";
          const trinoHeaders = query.trino_request_headers || {};

          // Pass through any user tags to Trino for resource group management
          const clientTags = new Set(query.tags);
          // Add custom tag so that queries can always be traced back to trino-proxy
          clientTags.add("trino-proxy");

          // Issue the statement request to the Trino cluster. This does not actually
          // kick off execution of the query, the first nextUri call does.
          const response = await axios({
            url: `${cluster.url}/v1/statement`,
            method: "post",
            headers: {
              // passthrough headers from client state
              ...trinoHeaders,
              // Overwrite the user, source, and tag headers with updated values
              "X-Trino-User": user,
              "X-Trino-Source": source,
              "X-Trino-Client-Tags": Array.from(clientTags).join(","),
            },
            data: query.body,
          });

          await knex("query")
            .transacting(trx)
            .where({ id: query.id })
            .update({
              cluster_query_id: response.data?.id,
              cluster_id: cluster.id,
              status: response.data?.stats?.state || QUERY_STATUS.QUEUED,
              next_uri: response.data?.nextUri || null,
              stats: response.data.stats,
            });

          logger.debug("Submitted query to Trino cluster", {
            queryId: query.id,
            cluster: cluster.name,
            user,
            source,
            data: response.data,
          });

          stats.increment("query_queued", [
            `cluster:${cluster.name}`,
            `user:${user}`,
            `source:${source}`,
            ...clientTags,
          ]);
        });
      } catch (scheduleErr) {
        logger.error("Error scheduling query", scheduleErr);
      }
    }
  } catch (err) {
    logger.error("Error scheduling queries", err);
  } finally {
    stats.timing("scheduler.timing", new Date() - startTime);
    schedulerRunning = false;
  }
}

async function runSchedulerAndReschedule() {
  await scheduleQueries();
  setTimeout(runSchedulerAndReschedule, SCHEDULER_DELAY_MS);
}

logger.info(`Scheduling query scheduler to run every ${SCHEDULER_DELAY_MS}ms`);
setTimeout(runSchedulerAndReschedule, SCHEDULER_DELAY_MS);

module.exports = {
  scheduleQueries,
  CLUSTER_STATUS,
};
