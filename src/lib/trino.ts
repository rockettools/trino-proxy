import _ from "lodash";
import axios, { isAxiosError } from "axios";
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

import type { Knex } from "knex";
import type { Cluster, Query } from "../types/models";
import type { ClusterInfo, ClusterStats } from "../types/trino";

type ClusterWithStats = Cluster & ClusterStats;

const SCHEDULER_MIN_DELAY_MS = parseInt(
  process.env.SCHEDULER_MIN_DELAY_MS || "1000"
);
const SCHEDULER_MAX_DELAY_MS = parseInt(
  process.env.SCHEDULER_MAX_DELAY_MS || "10000"
);
const ROUTING_METHOD =
  process.env.ROUTING_METHOD || ROUTING_METHODS.ROUND_ROBIN;
const DEFAULT_CLUSTER_TAG = process.env.DEFAULT_CLUSTER_TAG
  ? [process.env.DEFAULT_CLUSTER_TAG]
  : [];

let schedulerRunning = false;
let consecutiveFailures = 0;
const clusterHeaderRegex = new RegExp("-- Cluster: *(.*)");

/**
 * Checks that the Trino cluster is healthy and returns cluster stats.
 * The Trino http server starts before queries are ready to be queued, causing them to fail.
 * This function throws an error if the cluster is is not accessible or is initializing.
 */
async function getClusterStats(
  cluster: Cluster
): Promise<ClusterWithStats | null> {
  if (!cluster || !cluster.url) {
    throw new Error("Missing cluster");
  }

  let isHealthy = false;
  try {
    const infoResponse = await axios<ClusterInfo>({
      url: `${cluster.url}/v1/info`,
      method: "get",
    });

    // Cluster is healthy if `starting` property is false
    isHealthy = infoResponse.data && infoResponse.data.starting === false;
    if (!isHealthy) {
      return null;
    }

    const statsResponse = await axios<ClusterStats>({
      url: `${cluster.url}/ui/api/stats`,
      method: "get",
    });

    // TODO: check if queue is too long
    isHealthy = true;
    return {
      ...cluster,
      runningQueries: statsResponse.data.runningQueries,
      queuedQueries: statsResponse.data.queuedQueries,
      blockedQueries: statsResponse.data.blockedQueries,
    };
  } catch (err) {
    const errorDetails = isAxiosError(err) ? err.toJSON() : (err as Error);
    logger.error("Error checking cluster stats", {
      ...errorDetails,
      ...cluster,
    });

    return null;
  } finally {
    stats.increment("check_cluster_health", [
      `cluster:${cluster.name}`,
      `healthy:${isHealthy}`,
    ]);
  }
}

/**
 * Gets all available Trino clusters with stats
 */
async function getAvailableClusters(): Promise<ClusterWithStats[]> {
  const enabledClusters = await knex<Cluster>("cluster").where({
    status: CLUSTER_STATUS.ENABLED,
  });

  stats.gauge("available_clusters", enabledClusters.length);

  // Filter to only clusters that are healthy and ready for queries
  const healthyClusters: ClusterWithStats[] = [];
  await bluebird.each(enabledClusters, async (cluster) => {
    const clusterStats = await getClusterStats(cluster);
    if (clusterStats) {
      healthyClusters.push(clusterStats);
    }
    stats.increment("cluster_health", [
      `cluster:${cluster.name}`,
      `healthy:${Boolean(clusterStats)}`,
    ]);
  });

  stats.gauge("healthy_clusters", healthyClusters.length);
  return healthyClusters;
}

/**
 * Schedule a single query on the provided Trino cluster
 */
async function scheduleSingleQuery(
  query: Query,
  cluster: Cluster,
  knexTransaction: Knex.Transaction
) {
  const user = query.assumed_user || query.user;
  const source = query.source || "trino-proxy";
  const trinoHeaders = query.trino_request_headers || {};

  // Pass through any user tags to Trino for resource group management
  const clientTags = new Set(query.tags);
  // Add custom tag so that queries can always be traced back to trino-proxy
  clientTags.add("trino-proxy");

  try {
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
      .transacting(knexTransaction)
      .where({ id: query.id })
      .update({
        cluster_query_id: response.data?.id,
        cluster_id: cluster.id,
        status: response.data?.stats?.state || QUERY_STATUS.QUEUED,
        next_uri: response.data?.nextUri || null,
        stats: response.data.stats,
        updated_at: new Date(),
      });

    logger.debug("Submitted query to Trino cluster", {
      queryId: query.id,
      cluster: cluster.name,
      user: query.user,
      assumedUser: query.assumed_user,
    });
    stats.increment("query_queued", [
      `cluster:${cluster.name}`,
      `user:${user}`,
      `source:${source}`,
    ]);

    return true;
  } catch (scheduleErr) {
    if (isAxiosError(scheduleErr) && scheduleErr.status === 400) {
      await knex("query")
        .transacting(knexTransaction)
        .where({ id: query.id })
        .update({
          cluster_id: cluster.id,
          status: QUERY_STATUS.FAILED,
          updated_at: new Date(),
        });

      logger.error("Error submitting query", query);
      stats.increment("query_error", [
        `type:trino_error`,
        `user:${user}`,
        `source:${source}`,
      ]);
    } else {
      const errorDetails = isAxiosError(scheduleErr)
        ? scheduleErr.toJSON()
        : (scheduleErr as Error);
      logger.error("Error scheduling query", { ...query, ...errorDetails });
    }
  }

  return false;
}

/**
 * Schedules all pending queries
 */
async function scheduleQueries(): Promise<void> {
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
    if (numberQueriesPending === 0) {
      return;
    }

    const availableClusters = await getAvailableClusters();
    if (availableClusters.length === 0) {
      logger.error("No healthy clusters available");
      consecutiveFailures++;
      return;
    }

    logger.debug("Scheduling pending queries", {
      queries: queriesToSchedule.length,
      availableClusters: availableClusters.length,
    });

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

        // Scan the query for any targeted cluster tags
        const queryClusterTags = clusterHeaderRegex.exec(query.body) || [];
        const cluster = await chooseCluster(
          query,
          availableClusters,
          queryClusterTags
        );

        if (!cluster) {
          logger.error("No valid clusters found", query);
          stats.increment("query_error", [
            `type:no_valid_clusters`,
            `user:${query.assumed_user || query.user}`,
            `source:${query.source}`,
          ]);

          // If the query or user specified tags, then the failure is due to invalid
          // clusters being available and we need to fail the query instead of holding it
          if (queryClusterTags.length) {
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
            return;
          }

          // There are no clusters available, give them time to initialize
          consecutiveFailures++;
          return;
        }

        // If we've made it this far, we can schedule the query
        const querySucessful = await scheduleSingleQuery(query, cluster, trx);
        consecutiveFailures = querySucessful ? 0 : consecutiveFailures++;
      });
    } catch (scheduleErr) {
      // Trino errors are caught in `scheduleSingleQuery`, so
      // at this point we can leave the query and try again next loop
      logger.error("Error scheduling single query", scheduleErr);
    }
  } catch (err) {
    logger.error("Error scheduling queries", err);
  } finally {
    stats.timing("scheduler.timing", Date.now() - startTime);
    schedulerRunning = false;
  }
}

async function chooseCluster(
  query: Query,
  availableClusters: ClusterWithStats[] = [],
  queryClusterTags: string[] = []
): Promise<ClusterWithStats | undefined> {
  // Get user's cluster tags and default to DEFAULT_CLUSTER_TAG if no tags provided
  let queryUser = userCache.get(query.user);
  if (!queryUser) {
    queryUser = await knex("user").where({ id: query.user }).first();
    userCache.set(query.user, queryUser);
  }

  const userClusterTags =
    queryUser?.options?.clusterTags || DEFAULT_CLUSTER_TAG;

  // Filter down all clusters to just those that meet query's needs
  const validClusters = availableClusters.filter((cluster) => {
    const clusterTags = cluster.tags || [];

    // Clusters with tags are reserved for queries/users that target them
    if (clusterTags.length) {
      // If either the query or the user has tags, they must match with those of
      // the cluster; an absence of tags on the query or on the user may match
      // with any cluster, even if the cluster has tags.

      // Skip if query-specified tags and there's no overlap with cluster
      if (
        queryClusterTags.length > 0 &&
        !_.intersection(queryClusterTags, clusterTags).length
      ) {
        return false;
      }

      // Skip if user-specified tags and there's no overlap with cluster
      if (
        userClusterTags.length > 0 &&
        !_.intersection(userClusterTags, clusterTags).length
      ) {
        return false;
      }
    }

    // Include the cluster if nothing prevents us from skipping it
    return true;
  });

  if (ROUTING_METHOD === ROUTING_METHODS.ROUND_ROBIN) {
    return _.sample(validClusters);
  }

  if (ROUTING_METHOD === ROUTING_METHODS.LOAD) {
    const sortedClusters = validClusters.sort(compareByLoad);
    const cluster = sortedClusters[0];
    return cluster;
  }

  throw Error(`Unrecognized routing method: ${ROUTING_METHOD}`);
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

  const scheduleMs = _.clamp(
    consecutiveFailures * 1000,
    SCHEDULER_MIN_DELAY_MS,
    SCHEDULER_MAX_DELAY_MS
  );
  setTimeout(runSchedulerAndReschedule, scheduleMs);
}

export async function startScheduler() {
  if (!Object.values(ROUTING_METHODS).includes(ROUTING_METHOD)) {
    throw new Error(`Invalid routing method: ${ROUTING_METHOD}`);
  }

  // Give the service a little time to initialize before starting the loop
  const startDelay = 1000; //ms
  logger.info(`Starting scheduler in ${startDelay}ms`, {
    routingMethod: ROUTING_METHOD,
  });

  setTimeout(runSchedulerAndReschedule, startDelay);
}
