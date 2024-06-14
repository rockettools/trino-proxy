import axios from "axios";
import bluebird from "bluebird";
import { v4 as uuidv4 } from "uuid";

import {
  CLUSTER_STATUS,
  ROUTING_METHODS,
  TRINO_TEMP_HOST,
} from "../lib/constants";
import { knex } from "../lib/knex";
import logger from "../lib/logger";
import stats from "../lib/stats";
import { QUERY_STATUS } from "../lib/query";
import { createErrorResponseBody } from "../lib/helpers";
import { userCache } from "../lib/memcache";

import type { Cluster, Query } from "../types/models";
import type { ClusterStats } from "../types/trino";

const ROUTING_METHOD =
  process.env.ROUTING_METHOD || ROUTING_METHODS.ROUND_ROBIN;
const DEFAULT_CLUSTER_TAG = process.env.DEFAULT_CLUSTER_TAG
  ? [process.env.DEFAULT_CLUSTER_TAG]
  : [];

let schedulerRunning = false;
const SCHEDULER_DELAY_MS = process.env.SCHEDULER_DELAY_MS
  ? parseInt(process.env.SCHEDULER_DELAY_MS)
  : 1000 * 5;
const MAX_QUERIES_QUEUED = 10;

const clusterHeaderRegex = new RegExp("-- Cluster: *(.*)");

/**
 * Checks that the Trino cluster is finished initializing.
 * The Trino http server starts before queries are ready to be queued, causing them to fail.
 * This function throws an error if the cluster is is not accessible or is initializing.
 *
 * TODO: improve this to cache the server status or prefetch it to minimize time between
 * trino-proxy accepting a new query and it being passed off to a Trino cluster.
 */
async function isClusterHealthy(clusterBaseUrl?: string) {
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
    }
  );

  stats.gauge("available_clusters", availableClusters.length);
  return availableClusters;
}

async function scheduleQueries() {
  if (schedulerRunning) return;
  const startTime = Date.now();

  try {
    const queriesToSchedule = await knex("query")
      .where({ status: QUERY_STATUS.AWAITING_SCHEDULING })
      .count("*");

    // Increment number of pending queries for monitoring
    // If there are none, just early exit to prevent the extra db calls
    const numberQueriesPending = Number(queriesToSchedule[0].count) || 0;
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

          const cluster = await getCluster(
            availableClusters,
            currentClusterId,
            query
          );
          currentClusterId = (currentClusterId + 1) % availableClusters.length;

          if (!cluster) {
            logger.debug("No valid clusters found");
            if (clusterHeaderRegex.exec(query.body)) {
              // Create new error return to tell Trino client the query failed
              const response = createErrorResponseBody(
                query.id,
                uuidv4(),
                TRINO_TEMP_HOST,
                QUERY_STATUS.NO_VALID_CLUSTERS
              );

              await knex("query")
                .transacting(trx)
                .where({ id: query.id })
                .update({
                  status: QUERY_STATUS.NO_VALID_CLUSTERS,
                  error_info: response.data.error,
                });
            }
          } else {
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
            ]);
          }
        });
      } catch (scheduleErr) {
        // @ts-expect-error any type
        const errorDetails = scheduleErr?.toJSON?.() || scheduleErr;
        logger.error("Error scheduling query", errorDetails);
      }
    }
  } catch (err) {
    logger.error("Error scheduling queries", err);
  } finally {
    stats.timing("scheduler.timing", Date.now() - startTime);
    schedulerRunning = false;
  }
}

async function getCluster(
  availableClusters: Cluster[],
  currentClusterId: number,
  query: Query
) {
  // Get user's cluster tags and default to DEFAULT_CLUSTER_TAG if no tags provided
  let queryUser = userCache.get(query.user);
  if (!queryUser) {
    queryUser = await knex("user").where({ id: query.user }).first();
    userCache.set(query.user, queryUser);
  }

  const userClusterTags =
    queryUser?.options?.clusterTags || DEFAULT_CLUSTER_TAG;

  if (ROUTING_METHOD === ROUTING_METHODS.ROUND_ROBIN) {
    return availableClusters[currentClusterId];
  }

  if (ROUTING_METHOD === ROUTING_METHODS.LOAD) {
    // look to see if the user has passed a header in the query to target a cluster
    const queryClusterTags = clusterHeaderRegex.exec(query.body);

    const validClusters: Array<Cluster & ClusterStats> = [];

    for (const cluster of availableClusters) {
      if (queryClusterTags) {
        if (
          queryClusterTags.filter((x) => cluster.tags.includes(x)).length == 0
        )
          continue;
      } else {
        // skip cluster if the user's tags and cluster's tags do not intersect
        if (userClusterTags.filter((x) => cluster.tags.includes(x)).length == 0)
          continue;
      }

      const statsResponse = await axios<ClusterStats>({
        url: `${cluster.url}/ui/api/stats`,
        method: "get",
      });

      validClusters.push({
        ...cluster,
        runningQueries: statsResponse.data.runningQueries,
        queuedQueries: statsResponse.data.queuedQueries,
        blockedQueries: statsResponse.data.blockedQueries,
      });
    }

    logger.debug("sorting clusters based on load");
    const sortedClusters = validClusters.sort(compareByLoad);

    logger.debug("Sorted clusters", sortedClusters);
    const cluster = sortedClusters[0];
    return cluster;
  }
}

function compareByLoad(a: ClusterStats, b: ClusterStats) {
  return (
    a.runningQueries - b.runningQueries ||
    a.queuedQueries - b.queuedQueries ||
    a.blockedQueries - b.blockedQueries
  );
}

async function runSchedulerAndReschedule() {
  await scheduleQueries();
  setTimeout(runSchedulerAndReschedule, SCHEDULER_DELAY_MS);
}

export async function startScheduler() {
  logger.info("Starting scheduler", {
    periodMs: SCHEDULER_DELAY_MS,
    routingMethod: ROUTING_METHOD,
  });

  if (!Object.values(ROUTING_METHODS).includes(ROUTING_METHOD)) {
    throw new Error(`Invalid routing method: ${ROUTING_METHOD}`);
  }

  setTimeout(runSchedulerAndReschedule, SCHEDULER_DELAY_MS);
}
