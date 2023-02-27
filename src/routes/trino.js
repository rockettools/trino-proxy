const _ = require("lodash");
const express = require("express");
const uuidv4 = require("uuid").v4;
const axios = require("axios").default;

const { getAuthorizationHeader, getProxiedBody } = require("../lib/helpers");
const { knex } = require("../lib/knex");
const logger = require("../lib/logger");
const cache = require("../lib/memcache");
const {
  getQueryById,
  getQueryHeaderInfo,
  parseFirstQueryHeader,
  QUERY_STATUS,
  updateQuery,
} = require("../lib/query");

const { scheduleQueries } = require("../lib/trino");

const router = express.Router();
const TEMP_HOST = "http://localhost:5110"; // temp host later override to external host
const MOCKED_QUERY_KEY_ID = "AWAITING_SCHEDULING";
const MOCKED_QUERY_NUM = "0";

function getHost(req) {
  const host = req.get("host");
  const protocol = req.headers["x-forwarded-proto"]
    ? req.headers["x-forwarded-proto"] // TODO make sure this is only http or https
    : req.protocol;

  return `${protocol}://${host}`;
}

function getTrinoHeaders(headers = {}) {
  return _.pickBy(headers, (_value, key) => key.startsWith("x-trino"));
}

router.post("/v1/statement", async (req, res) => {
  if (!req.user) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  // Default user to whoever submitted the query
  let assumedUser = req.user.username;
  // Initialize empty client tags using a set to prevent duplicates
  const clientTags = new Set();

  try {
    const trinoTraceToken = req.headers["x-trino-trace-token"] || null;
    if (trinoTraceToken) {
      let info = await getQueryHeaderInfo(trinoTraceToken);

      // If this is the first query in the sequence it should have the header, try and parse.
      if (!info) {
        info = parseFirstQueryHeader(req.body, req.user.parsers);
        logger.debug("Parsed header info from query", info);
        cache.set(trinoTraceToken, info);
      }

      assumedUser = info.user;
      if (Array.isArray(info.tags)) {
        info.tags.forEach((t) => clientTags.add(t));
      }
    }

    // Add any custom user tags
    if (Array.isArray(req.user.tags)) {
      req.user.tags.forEach((t) => clientTags.add(t));
    }

    // Add any tags passed through in the X-Trino-Client-Tags header, which some clients provide
    const headerTags = req.headers["x-trino-client-tags"];
    if (headerTags) {
      const splitTags = headerTags.split(",");
      splitTags.forEach((t) => clientTags.add(t));
    }

    const newQueryId = uuidv4();
    const times = new Date();
    const querySource = req.headers["x-trino-source"] || null;

    await knex("query").insert({
      id: newQueryId,
      status: QUERY_STATUS.AWAITING_SCHEDULING,
      body: req.body,
      trino_request_headers: getTrinoHeaders(req.headers),
      trace_id: trinoTraceToken,
      assumed_user: assumedUser,
      source: querySource,
      tags: Array.from(clientTags),
      user: req.user.id || null,
      created_at: times,
      updated_at: times,
    });

    // Asynchronously schedule queries if not running already
    scheduleQueries();

    // Return a mock response with mocked query keyId and num until the query is scheduled and
    // a real URL is created for the client to call next. This service will continously return
    // this mocked nextUri until a valid one is available.
    const returnBody = getProxiedBody(
      {
        id: newQueryId,
        infoUri: `${TEMP_HOST}/ui/query.html?${newQueryId}`,
        nextUri: `${TEMP_HOST}/v1/statement/queued/${newQueryId}/${MOCKED_QUERY_KEY_ID}/${MOCKED_QUERY_NUM}`,
        stats: {
          state: QUERY_STATUS.QUEUED,
        },
      },
      newQueryId,
      getHost(req)
    );
    return res.status(200).json(returnBody);
  } catch (err) {
    logger.error("Error submitting statement", err);
    return res.status(500).json({ error: "A system error has occurred" });
  }
});

router.get("/v1/statement/:state/:queryId/:keyId/:num", async (req, res) => {
  const { state, queryId, keyId, num } = req.params;
  logger.debug("Fetching statement status", { state, queryId, keyId, num });
  if (state !== "queued" && state !== "executing") {
    logger.warn("Invalid statement status", { state, queryId });
    return res.status(400).json({ error: "Invalid query state" });
  }

  try {
    const query = await getQueryById(queryId);
    if (!query) {
      // If we are unable to find the queryMapping we're in trouble, fail the query
      logger.error("Query not found (check status)", { queryId });
      return res.status(404).json({ error: "Query not found" });
    }

    // If the query is in the AWAITING_SCHEDULING state, then it hasn't been sent to a
    // Trino cluster yet. Return a fake response with a mocked keyId until the query is scheduled.
    if (query.status === QUERY_STATUS.AWAITING_SCHEDULING) {
      const returnBody = getProxiedBody(
        {
          id: query.id,
          infoUri: `${TEMP_HOST}/ui/query.html?${query.id}`,
          nextUri: `${TEMP_HOST}/v1/statement/queued/${query.id}/${MOCKED_QUERY_KEY_ID}/${MOCKED_QUERY_NUM}`,
          stats: {
            state: QUERY_STATUS.QUEUED,
          },
        },
        query.id,
        getHost(req)
      );
      return res.status(200).json(returnBody);
    }

    // If we received the mocked keyId/num pair back and we're no longer in AWAITING_SCHEDULING,
    // we can use the NEXT_URI from the Trino cluster to create a valid URL
    if (keyId === MOCKED_QUERY_KEY_ID && num === MOCKED_QUERY_NUM) {
      const returnBody = getProxiedBody(
        {
          id: query.cluster_query_id,
          infoUri: `${TEMP_HOST}/ui/query.html?${query.id}`,
          nextUri: query.next_uri,
          stats: {
            state: query.status,
          },
        },
        query.id,
        getHost(req)
      );
      return res.status(200).json(returnBody);
    }

    const cluster = await knex("cluster")
      .where({ id: query.cluster_id })
      .first();

    const url = `${cluster.url}/v1/statement/${state}/${query.cluster_query_id}/${keyId}/${num}`;
    req.headers.authorization = await getAuthorizationHeader(req.headers);

    try {
      // Passthrough this request to the Trino cluster
      const response = await axios({
        url,
        method: "get",
        headers: req.headers,
      });

      await updateQuery(query.id, {
        status: response.data?.stats?.state,
        next_uri: response.data.nextUri || null,
      });

      const returnHeaders = getTrinoHeaders(response.headers);
      const returnBody = getProxiedBody(response.data, queryId, getHost(req));
      return res.status(200).set(returnHeaders).json(returnBody);
    } catch (err) {
      if (err.response && err.response.status === 404) {
        logger.error("Query not found on Trino cluster", {
          queryId,
          state,
          url,
        });

        // Update query status to lost
        await updateQuery(query.id, { status: QUERY_STATUS.LOST });
        return res.status(404).json({ error: "Query not found on cluster" });
      }
    }
  } catch (err) {
    logger.error("Error getting statement status", err, { params: req.params });
    return res.status(500).json({ error: "A system error has occurred" });
  }
});

router.delete("/v1/statement/:state/:queryId/:keyId/:num", async (req, res) => {
  const { state, queryId, keyId, num } = req.params;
  logger.debug("Cancelling query", { state, queryId, keyId, num });

  try {
    const query = await getQueryById(queryId);
    if (!query) {
      logger.error("Query not found (delete)", { queryId });
      return res.status(404).json({ error: "Query not found" });
    }

    // Update status and nextUri first to prevent the query
    // from being picked up by any database queries
    await updateQuery(query.id, {
      status: QUERY_STATUS.CANCELLED,
      next_uri: null,
    });

    const cluster = await knex("cluster")
      .where({ id: query.cluster_id })
      .first();

    const url = `${cluster.url}/v1/statement/${state}/${query.cluster_query_id}/${keyId}/${num}`;
    req.headers.authorization = await getAuthorizationHeader(req.headers);

    try {
      // Passthrough this deletion request to the Trino cluster to actually cancel the query
      await axios({ url, method: "delete", headers: req.headers });
      logger.info("Cancelled query on Trino cluster", { queryId });
    } catch (err) {
      // Anything other than a 404 error should be logged. If the query can't be found
      // then it's okay as we wanted to cancel it anyways.
      if (err.response && err.response.status !== 404) {
        logger.error("Query not found on Trino cluster", {
          queryId,
          state,
          url,
        });

        // Update query status to lost
        await updateQuery(query.id, { status: QUERY_STATUS.LOST });
        return res.status(404).json({ error: "Query not found on cluster" });
      }
    }

    return res.status(204).json({});
  } catch (err) {
    logger.error("Error cancelling statement", err, { params: req.params });
    return res.status(500).json({ error: "A system error has occurred" });
  }
});

module.exports = router;
