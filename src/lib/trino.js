const axios = require("axios").default;
const bluebird = require("bluebird");

const { CLUSTER_STATUS } = require("../lib/cluster");
const { knex } = require("../lib/knex");
const logger = require("../lib/logger");
const stats = require("../lib/stats");
const { QUERY_STATUS } = require("../lib/query");

let schedulerRunning = false;
const SCHEDULER_DELAY_MS = 1000 * 15;

async function scheduleQueries() {
  if (schedulerRunning) return;
  const startTime = new Date();

  try {
    const queriesToSchedule = await knex("query").where({
      status: QUERY_STATUS.AWAITING_SCHEDULING,
    });

    stats.increment("queries_waiting_scheduling", queriesToSchedule.length);
    if (queriesToSchedule.length === 0) return;

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
      }
    );

    stats.increment("available_clusters", availableClusters.length);
    if (availableClusters.length === 0) {
      logger.error("No healthy clusters available", {
        enabled: enabledClusters.length,
      });
      return;
    }

    logger.debug("Scheduling pending queries", {
      queries: queriesToSchedule.length,
      availableClusters: availableClusters.length,
    });

    let currentClusterId = Math.floor(Math.random() * availableClusters.length);
    for (const query of queriesToSchedule) {
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
        .where({ id: query.id })
        .update({
          cluster_query_id: response.data.id,
          cluster_id: cluster.id,
          status: response.data?.stats?.state || QUERY_STATUS.QUEUED,
          next_uri: response.data.nextUri,
        });

      logger.info("Submitted query to Trino cluster", {
        queryId: query.id,
        cluster: cluster.name,
        user,
        source,
        clientTags,
        data: response.data,
      });

      stats.increment("query_queued", [
        `cluster:${cluster.name}`,
        `user:${user}`,
        `source:${source}`,
        ...clientTags,
      ]);
    }
  } catch (err) {
    logger.error("Error scheduling queries", err);
  } finally {
    stats.timing("scheduler.timing", new Date() - startTime);
    schedulerRunning = false;
  }
}

/**
 * Checks that the Trino cluster is finished initializing. It appears that the http server starts
 * accepting queries before they're able to be queued, causing them to fail seconds later.
 * This function throws an error if the cluster is is not accessible or is initializing.
 *
 * TODO: improve this to cache the server status or prefetch it to minimize time between
 * trino-proxy accepting a new query and it being passed off to a Trino cluster.
 */
async function isClusterHealthy(clusterBaseUrl) {
  try {
    const response = await axios({
      url: `${clusterBaseUrl}/v1/info`,
      method: "get",
    });

    // Cluster is healthy if `starting` property is false
    return response.data && response.data.starting === false;
  } catch (err) {
    logger.error("Error checking cluster health", err, { clusterBaseUrl });
    return false;
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
};
