const express = require("express");
const axios = require("axios").default;
const uuidv4 = require("uuid").v4;
const http = require("http");
const https = require("https");
const { client } = require("./lib/redis");

const logger = require("./lib/logger");
const { getQueryById } = require("./lib/query");
const { updateUrls } = require("./lib/helpers");
//const { default: knex } = require("knex");

const { knex } = require("./lib/knex");

const {
  LISTEN_PORT = 8080,
  HTTPS_LISTEN_PORT = 8443,
  PRESTO_HOST,
  REDIS_URL,
} = process.env;
if (!PRESTO_HOST) throw new Error("PRESTO_HOST not set");
if (!REDIS_URL) throw new Error("REDIS_URL not set");

const app = express();

app.use(express.json()); // for parsing application/json
app.use(express.text()); // for parsing application/json

function getUsernameFromAuthorizationHeader(header) {
  if (header) {
    if (typeof header === "string") {
      header = [header];
    }
    for (let idx = 0; idx < header.length; idx++) {
      if (header[idx].indexOf("Basic ") === 0)
        return Buffer.from(header[idx].split(" ")[1], "base64").toString();
    }
  }
}

let schedulerRunning = false;
let runScheduler = false;
async function scheduleQueries() {
  console.log("Trying to schedule");
  if (schedulerRunning) {
    runScheduler = true;
    return;
  }
  try {
    const availableClusters = await knex("cluster").where({
      status: "enabled",
    });
    if (availableClusters.length === 0) {
      console.log("No clusters");
      return;
    }

    let currentClusterId = Math.floor(Math.random() * availableClusters.length);

    const queriesToSchedule = await knex("query").where({
      status: "awaiting_scheduling",
    });
    console.log("queriesToSchedule: " + currentClusterId, queriesToSchedule);
    for (let idx = 0; idx < queriesToSchedule.length; idx++) {
      const query = queriesToSchedule[idx];
      const cluster = availableClusters[currentClusterId];
      currentClusterId = (currentClusterId + 1) % availableClusters.length;
      await axios({
        url: cluster.url + "/v1/statement",
        method: "post",
        headers: { "X-Trino-User": query.assumed_user },
        data: query.body,
      }).then(async function (response) {
        //    const queryId = response.data.id;
        //     const newQueryId = uuidv4();
        console.log("Scheduled: ", response.data);

        const nextURI = response.data.nextUri.split(response.data.id + "/")[1];

        await knex("query").where({ id: query.id }).update({
          cluster_query_id: response.data.id,
          cluster_id: cluster.id,
          status: "queued",
          next_uri: nextURI,
        });

        // await saveQueryIdMapping(queryId, newQueryId);
        // const newBody = updateUrls(response.data, newQueryId, req.protocol);

        //  res.json(newBody);
      });
    }
  } catch (err) {
    console.log("ERROR", err);
  } finally {
    schedulerRunning = false;

    if (runScheduler) {
      runScheduler = false;
      scheduleQueries();
    }
  }
}

setInterval(scheduleQueries, 10000);

async function replaceAuthorizationHeader(req) {
  let headerUser;
  if (req.headers["x-trino-trace-token"]) {
    const info = await client.get(req.headers["x-trino-trace-token"]);
    console.log("Trace: " + req.headers["x-trino-trace-token"], info);

    // If this is the first query in the sequence it should have the header, try and parse.
    if (info) {
      headerUser = info;
    }
  }

  if (headerUser) {
    console.log("Replacing with: " + headerUser);
    req.headers.authorization =
      "Basic " + Buffer.from(headerUser).toString("base64");
  }
}

app.post("/v1/statement", async (req, res) => {
  logger.debug("Statement request", {});

  let headerUser;

  let trinoTraceToken = null;
  if (req.headers["x-trino-trace-token"]) {
    trinoTraceToken = req.headers["x-trino-trace-token"];
    const info = await client.get(trinoTraceToken);
    console.log("Trace: " + trinoTraceToken, info);

    // If this is the first query in the sequence it should have the header, try and parse.
    if (!info) {
      const newUsers = /-- user:(.*)$/gm.exec("-- user:skool\nasd");
      if (newUsers) headerUser = newUsers[1];

      await client.set(trinoTraceToken, "yes", {
        EX: 60 * 60,
      });
    } else {
      headerUser = info;
    }
  }

  if (headerUser) {
    req.headers.authorization =
      "Basic " + Buffer.from(headerUser).toString("base64");
  } else {
    const username = getUsernameFromAuthorizationHeader(
      req.headers.authorization
    );
    if (username) {
      req.headers.authorization = username;
    }
  }

  const newQueryId = uuidv4();

  await knex("query").insert({
    id: newQueryId,
    status: "awaiting_scheduling",
    body: req.body,
    trace_id: trinoTraceToken,
    assumed_user: headerUser,
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
      req.protocol + "://" + req.get("host")
    )
  );
});

app.get("/v1/statement/queued/:queryId/:keyId/:num", async (req, res) => {
  logger.debug("Statement fetching status 1", {});
  const query = await getQueryById(req.params.queryId);

  // If we are unable to find the queryMapping we're in trouble, fail the query.
  if (!query) {
    return res.status(404).json({
      error: "Query not found.",
    });
  }

  if (query.status === "awaiting_scheduling") {
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
        req.protocol + "://" + req.get("host")
      )
    );
  }

  if (req.params.keyId === "mock_next_uri") {
    console.log("Found mock_next_uri");
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
        req.protocol + "://" + req.get("host")
      )
    );
  }

  const cluster = await knex("cluster").where({ id: query.cluster_id }).first();

  await replaceAuthorizationHeader(req);
  axios({
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
  })
    .then(function (response) {
      const newBody = updateUrls(
        response.data,
        req.params.queryId,
        req.protocol + "://" + req.get("host")
      );

      res.json(newBody);
    })
    .catch(function (err) {
      if (err.response && err.response.status === 404) {
        console.log(
          "Query not found queueued: " +
            query.cluster_query_id +
            "/" +
            req.params.keyId +
            "/" +
            req.params.num
        );
        return res.status(404).send("Query not found");
      }
      console.log("3");
      process.exit(1);
      throw err;
    });
});

app.get("/v1/statement/executing/:queryId/:keyId/:num", async (req, res) => {
  logger.debug("Statement fetching status 2", {});

  const query = await getQueryById(req.params.queryId);
  // If we are unable to find the queryMapping we're in trouble, fail the query.
  if (!query) {
    return res.status(404).json({
      error: "Query not found.",
    });
  }

  const cluster = await knex("cluster").where({ id: query.cluster_id }).first();

  await replaceAuthorizationHeader(req);

  axios({
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
  })
    .then(function (response) {
      const newBody = updateUrls(
        response.data,
        req.params.queryId,
        req.protocol + "://" + req.get("host")
      );
      res.json(newBody);
    })
    .catch(function (err) {
      if (err.response && err.response.status === 404) {
        console.log(
          "Query not found when executing: " +
            query.cluster_query_id +
            "/" +
            req.params.keyId +
            "/" +
            req.params.num
        );
        return res.status(404).send("Query not found");
      }
      console.log("2");
      process.exit(1);
      throw err;
    });
});

app.use((req, res) => {
  logger.debug("Request data", { req });
  res.send("Hello World!");
});

async function main() {
  if (process.env.ENABLE_HTTPS) {
    const credentials = {
      key: process.env.HTTPS_KEY,
      cert: process.env.HTTPS_CERT,
    };
    var httpsServer = https.createServer(credentials, app, function () {
      logger.info(`Example app listening on port ${HTTPS_LISTEN_PORT}`);
    });
    httpsServer.listen(HTTPS_LISTEN_PORT);
    logger.info("Started");
  }

  if (!process.env.ENABLE_HTTPS || process.env.ENABLE_HTTP) {
    const httpServer = http.createServer(app, function () {
      logger.info(`Example app listening on port ${LISTEN_PORT}`);
    });
    httpServer.listen(LISTEN_PORT);
    logger.info("Started2");
  }
}

main();
