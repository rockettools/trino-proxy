const _ = require("lodash");
const url = require("url");
const axios = require("axios").default;

const { knex } = require("./knex");

const CLUSTER_STATUS = {
  ENABLED: "enabled",
  DISABLED: "disabled",
};

const stateMap = {
  FINISHED: "finished",
};

async function getClusterById(clusterId) {
  return knex("cluster").where({ id: clusterId }).first();
}

async function getSession(clusterId) {
  try {
    const cluster = await getClusterById(clusterId);
    const params = new url.URLSearchParams({
      username: "test",
      password: "",
      redirectPath: "",
    });

    await axios({
      url: cluster.url + "/ui/login",
      method: "post",
      headers: { "content-type": "application/x-www-form-urlencoded" },
      data: params.toString(),
      maxRedirects: 0,
    });
  } catch (err) {
    const setCookies = _.get(err, ["response", "headers", "set-cookie"]);
    console.log(setCookies);
    if (setCookies && setCookies.length > 0) {
      for (let idx = 0; idx < setCookies.length; idx++) {
        // This should be configurable based on the detected Trino version
        if (setCookies[idx].indexOf("Trino-UI-Token=") === 0) {
          const cookie = setCookies[idx].split(";")[0];
          return cookie;
        }
      }
    }
  }
}

async function getQueryStatus(clusterId, queryId) {
  // TODO cache these sessions somewhere and add in an auth check to handle expiration
  const session = await getSession(clusterId);
  const cluster = await getClusterById(clusterId);

  let result;
  try {
    result = await axios({
      url: cluster.url + "/ui/api/query/" + queryId,
      method: "get",
      headers: { cookie: session },
    });
  } catch (err) {
    const responseStatus = _.get(err, "response.status");
    if (responseStatus === 404 || responseStatus === 410) {
      return null;
    }

    throw err;
  }

  return {
    state: stateMap[result.data.state] || result.data.state,
    cumulativeUserMemoryMB: _.get(
      result.data,
      "queryStats.cumulativeUserMemory"
    ),

    // TODO: change this to a pick and a combine/merge
    elapsedTimeSeconds: mapToSeconds(
      _.get(result.data, "queryStats.elapsedTime")
    ),
    queuedTimeSeconds: mapToSeconds(
      _.get(result.data, "queryStats.queuedTime")
    ),
    resourceWaitingTimeSeconds: mapToSeconds(
      _.get(result.data, "queryStats.resourceWaitingTime")
    ),
    dispatchingTimeSeconds: mapToSeconds(
      _.get(result.data, "queryStats.dispatchingTime")
    ),
    executionTimeSeconds: mapToSeconds(
      _.get(result.data, "queryStats.executionTime")
    ),
    analysisTimeSeconds: mapToSeconds(
      _.get(result.data, "queryStats.analysisTime")
    ),
    planningTimeSeconds: mapToSeconds(
      _.get(result.data, "queryStats.planningTime")
    ),
    finishingTimeSeconds: mapToSeconds(
      _.get(result.data, "queryStats.finishingTime")
    ),

    createTime: _.get(result.data, "queryStats.createTime"),
    endTime: _.get(result.data, "queryStats.endTime"),
  };
}

// TODO: make this really map to seconds
function mapToSeconds(s) {
  return s;
}

module.exports = {
  getSession,
  getQueryStatus,
  CLUSTER_STATUS,
};
