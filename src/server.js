const express = require("express");
const http = require("http");
const https = require("https");
const { knex } = require("./lib/knex");
const argon2 = require("argon2");

const logger = require("./lib/logger");
//const { default: knex } = require("knex");

const {
  LISTEN_PORT = 8080,
  HTTPS_LISTEN_PORT = 8443,
  PRESTO_HOST,
} = process.env;
if (!PRESTO_HOST) throw new Error("PRESTO_HOST not set");

const app = express();

app.post("/v1/statement", function (req, res, next) {
  req.headers["content-type"] = "text/plain";
  next();
});

app.use(express.json()); // for parsing application/json
app.use(express.text()); // for parsing plain/text

app.use(async function (req, res, next) {
  console.log("EXTRA DEBUG: ", req);

  let username, password; // X-Trino-User

  if (req.headers["authorization"]) {
    let header = req.headers["authorization"];
    if (typeof header === "string") {
      header = [header];
    }
    for (let idx = 0; idx < header.length; idx++) {
      if (header[idx].indexOf("Basic ") === 0) {
        const foundHeader = Buffer.from(header[idx].split(" ")[1], "base64")
          .toString()
          .split(":");

        username = foundHeader[0];
        password = foundHeader[1];
        logger.debug("Found Auth header: " + username);

        // only bother with the first one
        break;
      }
    }
  } else if (req.headers["x-trino-user"]) {
    username = req.headers["x-trino-user"];
    logger.debug("Found Trino User header: " + username);
  }

  if (username) {
    const user = await knex("user")
      .where({
        name: username,
      })
      .first();

    if (user) {
      let rightPassword = false;

      if (!password && user.password && user.password.length > 0) {
        return res.status(401).send("Bad user/password");
      }

      // check all passwords to allow for password rotation
      for (let idx = 0; idx < user.password.length; idx++) {
        if (await argon2.verify(user.password[idx], password)) {
          rightPassword = true;
        }
      }

      if (!rightPassword && user.password.length !== 0) {
        return res.status(401).send("Bad user/password");
      } else {
        req.user = {
          id: user.id,
          username: username,
          parsers: user.parsers,
        };
      }
    }
  }

  next();
});

require("./routes")(app);

app.use((req, res) => {
  logger.debug("Request data", { req });
  res.send("Hello Trino!");
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
