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

function getHost(req) {
  return (
    (req.headers["x-forwarded-proto"]
      ? req.headers["x-forwarded-proto"] // TODO make sure this is only http or https
      : req.protocol) +
    "://" +
    req.get("host")
  );
}

router.post("/v1/statement", async (req, res) => {
  if (!req.user) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  logger.debug("Submitting statement", {
    user: req.user,
    query: process.env.LOG_QUERY ? req.body : "",
  });

  // TODO the assumedUser/real user pair should probably be locked for the trace set
  let assumedUser;

  const trinoTraceToken = req.headers["x-trino-trace-token"] || null;
  if (trinoTraceToken) {
    const info = await getAssumedUserForTrace(trinoTraceToken);
    logger.debug("Trace and already assumed user", { trinoTraceToken, info });

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
          state: "QUEUED",
        },
      },
      newQueryId,
      getHost(req)
    )
  );
});

router.get("/v1/statement/queued/:queryId/:keyId/:num", async (req, res) => {
  logger.debug("Fetching statement status: queued", {
    queryId: req.params.queryId,
    keyId: req.params.keyId,
    num: req.params.num,
  });

  const query = await getQueryById(req.params.queryId);

  // If we are unable to find the queryMapping we're in trouble, fail the query.
  if (!query) {
    return res.status(404).json({
      error: "Query not found.",
    });
  }

  if (query.status === QUERY_STATUS.AWAITING_SCHEDULING) {
    // TODO retur
    return res.json(
      updateUrls(
        {
          id: query.id,
          infoUri: "http://localhost:5110/ui/query.html?" + query.id,
          nextUri:
            "http://localhost:5110/v1/statement/queued/" +
            query.id +
            "/mock_next_uri/1",
          stats: {
            state: "QUEUED",
          },
        },
        query.id,
        getHost(req)
      )
    );
  }

  if (req.params.keyId === "mock_next_uri") {
    return res.json(
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
            state: "QUEUED",
          },
        },
        query.id,
        getHost(req)
      )
    );
  }

  const cluster = await knex("cluster").where({ id: query.cluster_id }).first();

  await replaceAuthorizationHeader(req);

  try {
    const response = await axios({
      url:
        cluster.url +
        "/v1/statement/queued/" +
        query.cluster_query_id +
        "/" +
        req.params.keyId +
        "/" +
        req.params.num,
      method: "get",
      headers: req.headers,
    });

    const newBody = updateUrls(response.data, req.params.queryId, getHost(req));
    return res.json(newBody);
  } catch (err) {
    if (err.response && err.response.status === 404) {
      logger.error("Query not found", {
        queryId: query.cluster_query_id,
        keyId: req.params.keyId,
        num: req.params.num,
      });

      return res.status(404).json({ error: "Query not found" });
    }
  }
});

router.get("/v1/statement/executing/:queryId/:keyId/:num", async (req, res) => {
  logger.debug("Fetching statement status: executing", {
    queryId: req.params.queryId,
    keyId: req.params.keyId,
    num: req.params.num,
  });

  const query = await getQueryById(req.params.queryId);
  // If we are unable to find the queryMapping we're in trouble, fail the query.
  if (!query) {
    return res.status(404).json({
      error: "Query not found",
    });
  }

  const cluster = await knex("cluster").where({ id: query.cluster_id }).first();

  await replaceAuthorizationHeader(req);

  try {
    const response = await axios({
      url:
        cluster.url +
        "/v1/statement/executing/" +
        query.cluster_query_id +
        "/" +
        req.params.keyId +
        "/" +
        req.params.num,
      method: "get",
      headers: req.headers,
    });

    const newBody = updateUrls(response.data, req.params.queryId, getHost(req));
    return res.json(newBody);
  } catch (err) {
    if (err.response && err.response.status === 404) {
      logger.error("Query not found when executing", {
        queryId: query.cluster_query_id,
        keyId: req.params.keyId,
        num: req.params.num,
      });

      return res.status(404).json({ error: "Query not found" });
    }
  }
});

module.exports = router;
