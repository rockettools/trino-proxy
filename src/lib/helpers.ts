import _ from "lodash";
import { getQueryHeaderInfo, QUERY_STATUS } from "./query";

import type { IncomingHttpHeaders } from "http";

export function getProxiedBody(
  clusterBody: any,
  proxyId: string,
  proxyHost: string
) {
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

export async function getAuthorizationHeader(headers: IncomingHttpHeaders) {
  const traceTokenHeader = headers["x-trino-trace-token"];
  const traceToken = Array.isArray(traceTokenHeader)
    ? traceTokenHeader[0]
    : traceTokenHeader;

  const headerUser = await getQueryHeaderInfo(traceToken);
  const authorizationHeader = headerUser?.user
    ? "Basic " + Buffer.from(headerUser.user).toString("base64")
    : null;

  return authorizationHeader;
}

export function createErrorResponseBody(
  queryId: string,
  uuid: string,
  tempHost: string,
  queryStatus: keyof typeof QUERY_STATUS
) {
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
