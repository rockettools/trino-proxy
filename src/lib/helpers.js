const _ = require("lodash");
const { getQueryHeaderInfo } = require("./query");

function getProxiedBody(clusterBody, proxyId, proxyHost) {
  const newBody = _.cloneDeep(clusterBody);
  // Save cluser's queryId for string replacement
  const clusterQueryId = newBody.id;
  // Overwrite cluster's queryId with that of trino-proxy
  newBody.id = proxyId;

  // Update infoUri link with proxy queryId and proxy host
  if (newBody.infoUri) {
    newBody.infoUri = newBody.infoUri
      .replace(clusterQueryId, proxyId)
      .replace(/https?:\/\/[^/]+/, proxyHost);
  }

  // Update nextUri link with proxy queryId and proxy host
  if (newBody.nextUri) {
    newBody.nextUri = newBody.nextUri
      .replace(clusterQueryId, proxyId)
      .replace(/https?:\/\/[^/]+/, proxyHost);
  }

  return newBody;
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

async function getAuthorizationHeader(headers) {
  const traceToken = headers["x-trino-trace-token"];
  const headerUser = await getQueryHeaderInfo(traceToken);
  const authorizationHeader = headerUser
    ? "Basic " + Buffer.from(headerUser).toString("base64")
    : null;

  return authorizationHeader;
}

module.exports = {
  getAuthorizationHeader,
  getProxiedBody,
  getUsernameFromAuthorizationHeader,
};
