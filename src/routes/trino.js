const uuidv4 = require("uuid").v4;
const logger = require("../lib/logger");
const { client } = require("../lib/redis");
const { getQueryById } = require("../lib/query");
const { knex } = require("../lib/knex");
const axios = require("axios").default;

const {
  getUsernameFromAuthorizationHeader,
  updateUrls,
  replaceAuthorizationHeader,
} = require("../lib/helpers");

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
        const nextURI = response.data.nextUri.split(response.data.id + "/")[1];

        await knex("query").where({ id: query.id }).update({
          cluster_query_id: response.data.id,
          cluster_id: cluster.id,
          status: "queued",
          next_uri: nextURI,
        });
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

module.exports = function (app) {
  app.post("/v1/statement", async (req, res) => {
    logger.debug("Statement request", {});

    let assumedUser;
    // let assumedGroups = [];

    let trinoTraceToken = null;
    if (req.headers["x-trino-trace-token"]) {
      trinoTraceToken = req.headers["x-trino-trace-token"];
      const info = await client.get(trinoTraceToken);
      console.log("Trace: " + trinoTraceToken, info);

      // If this is the first query in the sequence it should have the header, try and parse.
      if (!info) {
        if (req.user && req.user.parsers && req.user.parsers.user) {
          const parsedUser = new RegExp(req.user.parsers.user).exec(req.body);
          if (parsedUser) {
            assumedUser = parsedUser[1];
          }
        }

        // console.log("req.user.parsers", req.user.parsers);
        // if (req.user && req.user.parsers && req.user.parsers.groups) {
        //   //assumedGroups=[]
        //   const groupExp = new RegExp(req.user.parsers.groups, "gm");
        //   let m = groupExp.exec(req.body);
        //   console.log("group", m);
        //   while (m) {
        //     assumedGroups.push(m[req.user.parsers.groups_index || 1]);
        //     console.log("Group ", m);
        //     m = groupExp.exec(req.body);
        //   }
        //   // const parsedUser = new RegExp(req.user.parsers.user).exec(req.body);
        //   // if (parsedUser) {
        //   //   assumedUser = parsedUser[1];
        //   // }
        // }

        await client.set(trinoTraceToken, assumedUser, {
          EX: 60 * 60,
        });
      } else {
        assumedUser = info;
      }
    }

    if (assumedUser) {
      req.headers.authorization =
        "Basic " + Buffer.from(assumedUser).toString("base64");
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
      assumed_user: assumedUser,
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

    const cluster = await knex("cluster")
      .where({ id: query.cluster_id })
      .first();

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

    const cluster = await knex("cluster")
      .where({ id: query.cluster_id })
      .first();

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
};
