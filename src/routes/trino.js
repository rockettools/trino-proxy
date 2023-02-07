const _ = require("lodash");
const express = require("express");
const uuidv4 = require("uuid").v4;
const axios = require("axios").default;

const {
  getProxiedBody,
  replaceAuthorizationHeader,
} = require("../lib/helpers");
const { knex } = require("../lib/knex");
const logger = require("../lib/logger");
const cache = require("../lib/memcache");
const {
  getQueryById,
  getAssumedUserForTrace,
  updateQuery,
  QUERY_STATUS,
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

  try {
    // TODO the assumedUser/real user pair should probably be locked for the trace set
    let assumedUser;

    const trinoTraceToken = req.headers["x-trino-trace-token"] || null;
    if (trinoTraceToken) {
      const info = await getAssumedUserForTrace(trinoTraceToken);
      logger.silly("Trace and already assumed user", { trinoTraceToken, info });

      // If this is the first query in the sequence it should have the header, try and parse.
      if (!info) {
        if (req.user && req.user.parsers && req.user.parsers.user) {
          const parsedUser = new RegExp(req.user.parsers.user).exec(req.body);
          if (parsedUser) {
            assumedUser = parsedUser[1];
          }
        }

        cache.set(trinoTraceToken, assumedUser);
      } else {
        assumedUser = info;
      }
    }

    assumedUser = assumedUser || req.user.username;
    req.headers.authorization =
      "Basic " +
      Buffer.from(req.user.username + "__" + assumedUser).toString("base64");

    const newQueryId = uuidv4();
    const times = new Date();
    const querySource = req.headers["x-trino-source"] || null;

    // Querty tags are a combination of those from the user and passed through the headers
    const queryTags = new Set(req.user.tags);
    const headerTags = req.headers["x-trino-client-tags"];
    if (headerTags) {
      queryTags.add(...headerTags.split(","));
    }

    await knex("query").insert({
      id: newQueryId,
      status: QUERY_STATUS.AWAITING_SCHEDULING,
      body: req.body,
      trino_request_headers: getTrinoHeaders(req.headers),
      trace_id: trinoTraceToken,
      assumed_user: assumedUser,
      source: querySource,
      tags: Array.from(queryTags),
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

router.get("/v1/statement/queued/:queryId/:keyId/:num", async (req, res) => {
  const { queryId, keyId, num } = req.params;
  logger.debug("Fetching statement status: queued", { queryId, keyId, num });

  try {
    const query = await getQueryById(queryId);

    // If we are unable to find the queryMapping we're in trouble, fail the query.
    if (!query) {
      logger.error("Query not found (queued)", { queryId });
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

    await replaceAuthorizationHeader(req);

    try {
      // Passthrough this QUEUED request to the Trino cluster
      const response = await axios({
        url: `${cluster.url}/v1/statement/queued/${query.cluster_query_id}/${keyId}/${num}`,
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
        logger.error("Query not found on Trino cluster (statement queued)", {
          clusterId: query.cluster_query_id,
          queryId,
          keyId,
          num,
        });

        return res.status(404).json({ error: "Queued query not found" });
      }
    }
  } catch (err) {
    logger.error("Error statement queued", err);
    return res.status(500).json({ error: "A system error has occurred" });
  }
});

router.get("/v1/statement/executing/:queryId/:keyId/:num", async (req, res) => {
  const { queryId, keyId, num } = req.params;
  logger.debug("Fetching statement status: executing", { queryId, keyId, num });

  try {
    const query = await getQueryById(queryId);
    // If we are unable to find the queryMapping we're in trouble, fail the query.
    if (!query) {
      logger.error("Query not found (executing)", { queryId });
      return res.status(404).json({ error: "Query not found" });
    }

    const cluster = await knex("cluster")
      .where({ id: query.cluster_id })
      .first();

    await replaceAuthorizationHeader(req);

    try {
      // Passthrough this EXECUTING request to the Trino cluster
      const response = await axios({
        url: `${cluster.url}/v1/statement/executing/${query.cluster_query_id}/${keyId}/${num}`,
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
        logger.error("Query not found on Trino cluster (statement executing)", {
          clusterId: query.cluster_query_id,
          queryId,
          keyId,
          num,
        });

        return res.status(404).json({ error: "Executing query not found" });
      }
    }
  } catch (err) {
    logger.error("Error statement executing", err);
    return res.status(500).json({ error: "A system error has occurred" });
  }
});

module.exports = router;
