const _ = require("lodash");
const express = require("express");
const http = require("http");
const https = require("https");

const logger = require("./lib/logger");
const stats = require("./lib/stats");
const authenticationMiddleware = require("./middlewares/authentication");

const REQUEST_BODY_LIMIT = process.env.REQUEST_BODY_LIMIT || "500kb";
const HTTP_ENABLED = process.env.HTTP_ENABLED === "true";
const HTTP_LISTEN_PORT = parseInt(process.env.HTTP_LISTEN_PORT) || 8080;
const HTTPS_ENABLED = process.env.HTTPS_ENABLED === "true";
const HTTPS_LISTEN_PORT = parseInt(process.env.HTTPS_LISTEN_PORT) || 8443;

const app = express();

// Set content-type for incoming statement requests before parsing middleware is applied
app.post("/v1/statement", (req, res, next) => {
  req.headers["content-type"] = "text/plain";
  next();
});

app.use(express.json({ limit: REQUEST_BODY_LIMIT })); // for parsing application/json
app.use(express.text({ limit: REQUEST_BODY_LIMIT })); // for parsing plain/text

// Middleware: authentication
app.use(authenticationMiddleware);
// Middleware: log request data
app.use((req, _res, next) => {
  logger.debug(
    "Request data",
    _.pick(req, ["url", "body", "params", "query", "rawHeaders"]),
  );
  next();
});

// Add routes
app.use("/", require("./routes/cluster"));
app.use("/", require("./routes/query"));
app.use("/", require("./routes/trino"));
app.use("/", require("./routes/user"));

// Health check
app.get("/health", (_req, res) => {
  stats.increment("healthcheck");
  return res.status(200).json({ status: "ok" });
});

// Mock info endpoint to abstract out the cluster information
app.get("/v1/info", async (req, res) => {
  return res.status(200).json({
    nodeVersion: { version: "trino-proxy" },
    environment: "docker",
    coordinator: true,
    starting: false,
  });
});

// Fallback handler
app.use("*", (req, res) => {
  logger.warn("No matching route", _.pick(req, ["url", "body"]));
  return res.status(404).json({ error: "Unknown route" });
});

// Setup server and start listening for requests
if (HTTPS_ENABLED) {
  const credentials = {
    key: process.env.HTTPS_KEY,
    cert: process.env.HTTPS_CERT,
  };
  const httpsServer = https.createServer(credentials, app);
  httpsServer.listen(HTTPS_LISTEN_PORT);
  logger.info(`HTTPS server listen on port ${HTTPS_LISTEN_PORT}`);
}

if (!HTTPS_ENABLED || HTTP_ENABLED) {
  const httpServer = http.createServer(app);
  httpServer.listen(HTTP_LISTEN_PORT);
  logger.info(`HTTP server listen on port ${HTTP_LISTEN_PORT}`);
}

logger.info('Routing method: ' + process.env.ROUTING_METHOD);
