const axios = require("axios").default;

const { CLUSTER_STATUS } = require("../lib/cluster");
const { knex } = require("../lib/knex");
const logger = require("../lib/logger");
const stats = require("../lib/stats");
const { QUERY_STATUS } = require("../lib/query");

let schedulerRunning = false;

async function scheduleQueries() {
  if (schedulerRunning) return;

  const startTime = new Date();
  logger.info("Scheduling pending queries");

  try {
    const [availableClusters, queriesToSchedule] = await Promise.all([
      knex("cluster").where({ status: CLUSTER_STATUS.ENABLED }),
      knex("query").where({ status: QUERY_STATUS.AWAITING_SCHEDULING }),
    ]);

    stats.histogram("available_clusters", availableClusters.length);
    stats.histogram("queries_waiting_scheduling", queriesToSchedule.length);
    logger.debug("Queries awaiting scheduling", {
      queries: queriesToSchedule.length,
      clusters: availableClusters.length,
    });

    if (availableClusters.length === 0) {
      logger.error("No enabled clusters available");
      return;
    }

    let currentClusterId = Math.floor(Math.random() * availableClusters.length);
    for (let idx = 0; idx < queriesToSchedule.length; idx++) {
      const query = queriesToSchedule[idx];
      const cluster = availableClusters[currentClusterId];
      currentClusterId = (currentClusterId + 1) % availableClusters.length;
      logger.debug("Submitting query", {
        id: query.id,
        url: cluster.url,
        user: query.assumed_user,
        source: query.source,
        currentClusterId,
      });

      const userTags = Array.isArray(query.tags) ? query.tags : [];
      const clientTags = userTags.concat("trino-proxy");

      const response = await axios({
        url: cluster.url + "/v1/statement",
        method: "post",
        headers: {
          "X-Trino-User": query.assumed_user,
          "X-Trino-Source": query.source || "trino-proxy",
          "X-Trino-Client-Tags": clientTags.join(","),
        },
        data: query.body,
      });

      logger.debug("Trino cluster response", { data: response.data });
      const nextURI = response.data.nextUri.split(response.data.id + "/")[1];
      await knex("query").where({ id: query.id }).update({
        cluster_query_id: response.data.id,
        cluster_id: cluster.id,
        status: QUERY_STATUS.QUEUED,
        next_uri: nextURI,
      });
    }
  } catch (err) {
    logger.error("Error scheduling queries", err);
  } finally {
    stats.timing("scheduler.timing", new Date() - startTime);
    schedulerRunning = false;
  }
}

async function runScheduledScheduleQueries() {
  await scheduleQueries();
  setTimeout(runScheduledScheduleQueries, 10000);
}

setTimeout(runScheduledScheduleQueries, 10000);

module.exports = {
  scheduleQueries,
};
