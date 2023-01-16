const _ = require("lodash");
const express = require("express");
const uuidv4 = require("uuid").v4;

const { axios } = require("../lib/axios");
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

  logger.info("Submitting statement", {
    user: req.user,
    query: process.env.LOG_QUERY ? req.body : "",
  });

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

    return res.json(
      updateUrls(
        {
          id: newQueryId,
          infoUri: "http://localhost:5110/ui/query.html?" + newQueryId,
          nextUri:
            "http://localhost:5110/v1/statement/queued/" +
            newQueryId +
            "/mock_next_uri/1",
          stats: {
            state: QUERY_STATUS.QUEUED,
          },
        },
        newQueryId,
        getHost(req)
      )
    );
  } catch (err) {
    logger.error("Error submitting statement", err);
    return res.status(500).json({ error: "A system error has occured" });
  }
});

router.get("/v1/statement/queued/:queryId/:keyId/:num", async (req, res) => {
  const { queryId, keyId, num } = req.params;
  logger.debug("Fetching statement status: queued", { queryId, keyId, num });

  try {
    const query = await getQueryById(queryId);

    // If we are unable to find the queryMapping we're in trouble, fail the query.
    if (!query) {
      return res.status(404).json({ error: "Query not found" });
    }

    if (query.status === QUERY_STATUS.AWAITING_SCHEDULING) {
      return res.status(200).json(
        updateUrls(
          {
            id: query.id,
            infoUri: "http://localhost:5110/ui/query.html?" + query.id,
            nextUri:
              "http://localhost:5110/v1/statement/queued/" +
              query.id +
              "/mock_next_uri/1",
            stats: {
              state: QUERY_STATUS.QUEUED,
            },
          },
          query.id,
          getHost(req)
        )
      );
    }

    if (keyId === "mock_next_uri") {
      return res.status(200).json(
        updateUrls(
          {
            id: query.id,
            infoUri: "http://localhost:5110/ui/query.html?" + query.id,
            nextUri:
              "http://localhost:5110/v1/statement/queued/" +
              query.id +
              "/" +
              query.next_uri,
            stats: {
              state: QUERY_STATUS.QUEUED,
            },
          },
          query.id,
          getHost(req)
        )
      );
    }

    const cluster = await knex("cluster")
      .where({ id: query.cluster_id })
      .first();

    await replaceAuthorizationHeader(req);

    try {
      const response = await axios({
        url:
          cluster.url +
          "/v1/statement/queued/" +
          query.cluster_query_id +
          "/" +
          keyId +
          "/" +
          num,
        method: "get",
        headers: req.headers,
      });

      // If state changed, update in database
      const newState = _.get(response, "data.stats.state");
      if (newState && newState !== query.status) {
        await knex("query")
          .where("id", query.id)
          .update({ status: newState, updated_at: new Date() });
      }

      const newBody = updateUrls(response.data, queryId, getHost(req));
      return res.status(200).json(newBody);
    } catch (err) {
      if (err.response && err.response.status === 404) {
        logger.error("Query not found (statement queued)", {
          clusterId: query.cluster_query_id,
          queryId,
          keyId,
          num,
        });

        await knex("query")
          .where("id", query.id)
          .update({ status: QUERY_STATUS.LOST, updated_at: new Date() });

        return res.status(404).json({ error: "Query not found" });
      }
    }
  } catch (err) {
    logger.error("Error statement queued", err);
    return res.status(500).json({ error: "A system error has occured" });
  }
});

router.get("/v1/statement/executing/:queryId/:keyId/:num", async (req, res) => {
  const { queryId, keyId, num } = req.params;
  logger.debug("Fetching statement status: executing", { queryId, keyId, num });

  try {
    const query = await getQueryById(queryId);
    // If we are unable to find the queryMapping we're in trouble, fail the query.
    if (!query) {
      return res.status(404).json({
        error: "Query not found",
      });
    }

    const cluster = await knex("cluster")
      .where({ id: query.cluster_id })
      .first();

    await replaceAuthorizationHeader(req);

    try {
      const response = await axios({
        url:
          cluster.url +
          "/v1/statement/executing/" +
          query.cluster_query_id +
          "/" +
          keyId +
          "/" +
          num,
        method: "get",
        headers: req.headers,
      });

      // If state changed, update in database
      const newState = _.get(response, "data.stats.state");
      if (newState && newState !== query.status) {
        await knex("query")
          .where("id", query.id)
          .update({ status: newState, updated_at: new Date() });
      }

      const newBody = updateUrls(response.data, queryId, getHost(req));
      return res.status(200).json(newBody);
    } catch (err) {
      if (err.response && err.response.status === 404) {
        logger.error("Query not found (statement executing)", {
          clusterId: query.cluster_query_id,
          queryId,
          keyId,
          num,
        });

        await knex("query")
          .where("id", query.id)
          .update({ status: QUERY_STATUS.LOST, updated_at: new Date() });

        return res.status(404).json({ error: "Query not found" });
      }
    }
  } catch (err) {
    logger.error("Error statement executing", err);
    return res.status(500).json({ error: "A system error has occured" });
  }
});

module.exports = router;
