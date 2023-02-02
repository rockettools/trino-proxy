const logger = require("./logger");
const { getAssumedUserForTrace } = require("./query");

function updateUrls(body, newQueryId, host) {
  const queryId = body.id;
  body.id = newQueryId;

  if (body.infoUri) {
    body.infoUri = body.infoUri
      .replace(queryId, newQueryId)
      .replace(/https?:\/\/[^/]+/, host);
  }
  if (body.nextUri) {
    body.nextUri = body.nextUri
      .replace(queryId, newQueryId)
      .replace(/https?:\/\/[^/]+/, host);
  }
  return body;
}

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

async function replaceAuthorizationHeader(req) {
  let headerUser;
  if (req.headers["x-trino-trace-token"]) {
    headerUser = await getAssumedUserForTrace(
      req.headers["x-trino-trace-token"]
    );
  }

  if (headerUser) {
    logger.silly("Replacing authorization header", { headerUser });
    req.headers.authorization =
      "Basic " + Buffer.from(headerUser).toString("base64");
  }
}

module.exports = {
  getUsernameFromAuthorizationHeader,
  replaceAuthorizationHeader,
  updateUrls,
};
