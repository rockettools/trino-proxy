import _ from "lodash";
import express from "express";
import http from "http";
import https from "https";

import logger from "./lib/logger";
import stats from "./lib/stats";
import authenticationMiddleware from "./middlewares/authentication";
import { startScheduler } from "./lib/trino";

import clusterRouter from "./routes/cluster";
import queryRouter from "./routes/query";
import trinoRouter from "./routes/trino";
import userRouter from "./routes/user";

const ENABLE_API = process.env.ENABLE_API === "true";
const ENABLE_SCHEDULER = process.env.ENABLE_SCHEDULER === "true";
const REQUEST_BODY_LIMIT = process.env.REQUEST_BODY_LIMIT || "500kb";
const HTTP_ENABLED = process.env.HTTP_ENABLED === "true";
const HTTP_LISTEN_PORT = process.env.HTTP_LISTEN_PORT
  ? parseInt(process.env.HTTP_LISTEN_PORT)
  : 8080;
const HTTPS_ENABLED = process.env.HTTPS_ENABLED === "true";
const HTTPS_LISTEN_PORT = process.env.HTTPS_LISTEN_PORT
  ? parseInt(process.env.HTTPS_LISTEN_PORT)
  : 8443;

// Validate that at least one of the services is enabled. One or both can be set.
if (!ENABLE_API && !ENABLE_SCHEDULER) {
  throw new Error("Please enable API and/or Scheduler services");
}

// Basic server, which is setup for both the API and Scheduler
// In both microservices, health checks are required
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
    _.pick(req, ["url", "body", "params", "query", "rawHeaders"])
  );
  next();
});

// Health check
app.get("/health", (_req, res) => {
  stats.increment("healthcheck");
  return res.status(200).json({ status: "ok" });
});

// Enable Trino Proxy and Trino client APIs
if (ENABLE_API) {
  app.use("/", clusterRouter);
  app.use("/", queryRouter);
  app.use("/", trinoRouter);
  app.use("/", userRouter);
}

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

// In scheduler is enabled, start the schedule loop
if (ENABLE_SCHEDULER) {
  startScheduler();
}
