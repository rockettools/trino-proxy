const axios = require("axios").default;

const { CLUSTER_STATUS } = require("../lib/cluster");
const { knex } = require("../lib/knex");
const logger = require("../lib/logger");
const stats = require("../lib/stats");
const { QUERY_STATUS } = require("../lib/query");

let schedulerRunning = false;
let runScheduler = false;

async function scheduleQueries() {
  logger.debug("Scheduling pending queries");
  if (schedulerRunning) {
    runScheduler = true;
    return;
  }

  try {
    const availableClusters = await knex("cluster").where({
      status: CLUSTER_STATUS.ENABLED,
    });
    if (availableClusters.length === 0) {
      logger.error("No clusters");
      return;
    }

    let currentClusterId = Math.floor(Math.random() * availableClusters.length);

    const queriesToSchedule = await knex("query").where({
      status: QUERY_STATUS.AWAITING_SCHEDULING,
    });

    stats.histogram("queries_waiting_scheduling", queriesToSchedule.length);

    for (let idx = 0; idx < queriesToSchedule.length; idx++) {
      const query = queriesToSchedule[idx];
      const cluster = availableClusters[currentClusterId];
      currentClusterId = (currentClusterId + 1) % availableClusters.length;
      const userTags = Array.isArray(query.tags) ? query.tags : [];
      // Add custom tag so that queries can always be traced back to trino-proxy
      const clientTags = userTags.concat("trino-proxy");

      logger.debug("Submitting query", {
        id: query.id,
        url: cluster.url,
        user: query.assumed_user,
        source: query.source,
        clientTags,
        currentClusterId,
      });

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
    schedulerRunning = false;

    if (runScheduler) {
      runScheduler = false;
      scheduleQueries();
    }
  }
}

async function runScheduledScheduleQueries() {
  try {
    await scheduleQueries();
  } catch (err) {
    logger.error("Error scheduling queries", err);
  }

  setTimeout(runScheduledScheduleQueries, 10000);
}

setTimeout(runScheduledScheduleQueries, 10000);

module.exports = {
  scheduleQueries,
};
