const express = require("express");
const uuidv4 = require("uuid").v4;
const axios = require("axios").default;

const { updateUrls, replaceAuthorizationHeader } = require("../lib/helpers");
const { knex } = require("../lib/knex");
const logger = require("../lib/logger");
const cache = require("../lib/memcache");
const {
  getQueryById,
  getAssumedUserForTrace,
  QUERY_STATUS,
} = require("../lib/query");

const { scheduleQueries } = require("../lib/trino");

const router = express.Router();
const MOCKED_NEXT_URI = "mock_next_uri";

function getHost(req) {
  const host = req.get("host");
  const protocol = req.headers["x-forwarded-proto"]
    ? req.headers["x-forwarded-proto"] // TODO make sure this is only http or https
    : req.protocol;

  return `${protocol}://${host}`;
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

    await knex("query").insert({
      id: newQueryId,
      status: QUERY_STATUS.AWAITING_SCHEDULING,
      body: req.body,
      trace_id: trinoTraceToken,
      assumed_user: assumedUser,
      source: querySource,
      tags: req.user.tags || [],
      user: req.user.id || null,
      created_at: times,
      updated_at: times,
    });

    // Asynchronously schedule queries if not running already
    scheduleQueries();

    const returnBody = updateUrls(
      {
        id: newQueryId,
        infoUri: `http://localhost:5110/ui/query.html?${newQueryId}`,
        nextUri: `http://localhost:5110/v1/statement/queued/${newQueryId}/${MOCKED_NEXT_URI}/1`,
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
      const returnBody = updateUrls(
        {
          id: query.id,
          infoUri: `http://localhost:5110/ui/query.html?${query.id}`,
          nextUri: `http://localhost:5110/v1/statement/queued/${query.id}/${MOCKED_NEXT_URI}/1`,
          stats: {
            state: QUERY_STATUS.QUEUED,
          },
        },
        query.id,
        getHost(req)
      );
      return res.status(200).json(returnBody);
    }

    // If we received the mocked keyId back and we're no longer in AWAITING_SCHEDULING, then
    // we can use the NEXT_URI from the Trino cluster to create a valid URL
    if (keyId === MOCKED_NEXT_URI) {
      const returnBody = updateUrls(
        {
          id: query.id,
          infoUri: `http://localhost:5110/ui/query.html?${query.id}`,
          nextUri: `http://localhost:5110/v1/statement/queued/${query.id}/${query.next_uri}`,
          stats: {
            state: QUERY_STATUS.QUEUED,
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
      const response = await axios({
        url: `${cluster.url}/v1/statement/queued/${query.cluster_query_id}/${keyId}/${num}`,
        method: "get",
        headers: req.headers,
      });

      const returnBody = updateUrls(response.data, queryId, getHost(req));
      return res.status(200).json(returnBody);
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
      const response = await axios({
        url: `${cluster.url}/v1/statement/executing/${query.cluster_query_id}/${keyId}/${num}`,
        method: "get",
        headers: req.headers,
      });

      const returnBody = updateUrls(response.data, queryId, getHost(req));
      return res.status(200).json(returnBody);
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
