const express = require("express");
const http = require("http");
const https = require("https");

const logger = require("./lib/logger");
const authenticationMiddleware = require("./middlewares/authentication");

const HTTP_ENABLED = process.env.HTTP_ENABLED === "true";
const HTTP_LISTEN_PORT = parseInt(process.env.HTTP_LISTEN_PORT) || 8080;
const HTTPS_ENABLED = process.env.HTTPS_ENABLED === "true";
const HTTPS_LISTEN_PORT = parseInt(process.env.HTTPS_LISTEN_PORT) || 8443;

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

// Setup server and start listening for requests
if (HTTPS_ENABLED) {
  const credentials = {
    key: process.env.HTTPS_KEY,
    cert: process.env.HTTPS_CERT,
  };
  const httpsServer = https.createServer(credentials, app);
  httpsServer.listen(HTTPS_LISTEN_PORT);
}

if (!HTTPS_ENABLED || HTTP_ENABLED) {
  const httpServer = http.createServer(app);
  httpServer.listen(HTTP_LISTEN_PORT);
}

// Require babysitter last once server is setup and running successfully
require("./babysitter");
