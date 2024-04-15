const _ = require("lodash");
const { getQueryHeaderInfo } = require("./query");

function getProxiedBody(clusterBody, proxyId, proxyHost) {
  const newBody = _.cloneDeep(clusterBody);
  // Save cluster's queryId for string replacement
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

async function getAuthorizationHeader(headers) {
  const traceToken = headers["x-trino-trace-token"];
  const headerUser = await getQueryHeaderInfo(traceToken);
  const authorizationHeader = headerUser
    ? "Basic " + Buffer.from(headerUser).toString("base64")
    : null;

  return authorizationHeader;
}

function createErrorResponseBody(queryId, uuid, tempHost, queryStatus) {
  return {
    data: {
      id: uuid,
      infoUri: `${tempHost}/ui/query.html?${queryId}`,
      stats: {
        state: queryStatus,
      },
      error: {
        message:
          "No valid clusters found for user cluster tag / query cluster header",
        errorCode: -1,
        errorName: queryStatus,
        errorType: "USER_ERROR",
        errorLocation: {
          lineNumber: 1,
          columnNumber: 1,
        },
        failureInfo: {
          type: "no valid clusters",
          message:
            "No valid clusters found for user cluster tag / query cluster header",
          stack: [],
          suppressed: [],
          errorInfo: {
            code: -1,
            name: queryStatus,
            type: "USER_ERROR",
          },
          errorLocation: {
            lineNumber: 1,
            columnNumber: 1,
          },
        },
      },
      warnings: [],
    },
  };
}

module.exports = {
  getAuthorizationHeader,
  getProxiedBody,
  createErrorResponseBody,
};
