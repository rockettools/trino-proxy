const express = require("express");
const http = require("http");
const https = require("https");

const logger = require("./lib/logger");
const authenticationMiddleware = require("./middlewares/authentication");

const LISTEN_PORT = parseInt(process.env.LISTEN_PORT) || 8080;
const HTTPS_LISTEN_PORT = parseInt(process.env.HTTPS_LISTEN_PORT) || 8443;
const ENABLE_HTTP = process.env.ENABLE_HTTP === "true";
const ENABLE_HTTPS = process.env.ENABLE_HTTPS === "true";
const DISABLE_BABYSITTER = process.env.DISABLE_BABYSITTER === "true";

const app = express();

app.post("/v1/statement", function (req, res, next) {
  req.headers["content-type"] = "text/plain";
  next();
});

app.use(express.json()); // for parsing application/json
app.use(express.text()); // for parsing plain/text

// Middleware: authentication
app.use(authenticationMiddleware);
// Middleware: log request data and set default response
app.use((req, res) => {
  logger.debug("Request data", { req });
  res.json({});
});

// Add routes
require("./routes/index")(app);

if (ENABLE_HTTPS) {
  const credentials = {
    key: process.env.HTTPS_KEY,
    cert: process.env.HTTPS_CERT,
  };
  const httpsServer = https.createServer(credentials, app);
  httpsServer.listen(HTTPS_LISTEN_PORT);
}

if (!ENABLE_HTTPS || ENABLE_HTTP) {
  const httpServer = http.createServer(app);
  httpServer.listen(LISTEN_PORT);
}

if (!DISABLE_BABYSITTER) {
  require("./babysitter")();
}
