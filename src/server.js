const { createClient } = require("redis");
const express = require("express");
const axios = require("axios").default;
const uuidv4 = require("uuid").v4;

const { REDIS_URL } = process.env;
if (!REDIS_URL) {
  throw new Error("REDIS_URL not set");
}

const client = createClient({ url: REDIS_URL });
client.on("error", (err) => console.log("Redis Client Error", err));

const app = express();
const port = 8080;
const presto_url = "http://localhost:5110";

app.use(express.json()); // for parsing application/json
app.use(express.text()); // for parsing application/json

function updateUrls(body, newQueryId) {
  const queryId = body.id;
  body.id = newQueryId;

  if (body.infoUri) {
    body.infoUri = body.infoUri.replace(queryId, newQueryId);
  }
  if (body.nextUri) {
    body.nextUri = body.nextUri.replace(queryId, newQueryId);
  }
  return body;
}

app.post("/v1/statement", async (req, res) => {
  console.log(req.headers);
  console.log(req.body);

  axios({
    url: presto_url + "/v1/statement",
    method: "post",
    headers: req.headers,
    data: req.body,
  }).then(async function (response) {
    const queryId = response.data.id;
    const newQueryId = uuidv4();

    await client.set(newQueryId, queryId, {
      EX: 60 * 60,
      // NX: true
    });

    //console.log(response.data);
    // process.exit(1)

    res.json(response.data);
  });
});

app.get("/v1/statement/queued/:queryId/:keyId/:num", async (req, res) => {
  // console.log("Trouble");
  const newQueryId = await client.get(req.params.queryId);
  console.log("1Trouble", req.params.queryId);

  console.log("2Trouble", newQueryId);
  if (!newQueryId) process.exit(1);
  axios({
    url:
      presto_url +
      "/v1/statement/queued/" +
      newQueryId +
      "/" +
      req.params.keyId +
      "/" +
      req.params.num,
    method: "get",
    headers: req.headers,
  }).then(function (response) {
    const newBody = updateUrls(response.data, req.params.queryId);

    res.json(newBody);
  });
});

app.get("/v1/statement/executing/:queryId/:keyId/:num", async (req, res) => {
  const newQueryId = await client.get(req.params.queryId);
  console.log("1Trouble", req.params.queryId);

  console.log("2Trouble", newQueryId);
  if (!newQueryId) {
    process.exit(1);
    // didn't find query id
  }
  axios({
    url:
      presto_url +
      "/v1/statement/executing/" +
      newQueryId +
      "/" +
      req.params.keyId +
      "/" +
      req.params.num,
    method: "get",
    headers: req.headers,
  }).then(function (response) {
    const newBody = updateUrls(response.data, req.params.queryId);
    res.json(newBody);
  });
});

app.use((req, res) => {
  console.log(req.method);

  console.log(req.path);
  console.log(req.headers);
  console.log(req.text);

  console.log(req.body);
  res.send("Hello World!");
});

async function main() {
  await client.connect();

  app.listen(port, () => {
    console.log(`Example app listening on port ${port}`);
  });
}

main();
