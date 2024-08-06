import _ from "lodash";
import { QUERY_STATUS } from "./query";

import type { ClusterResponse } from "../types/trino";

export function getProxiedBody(
  clusterBody: ClusterResponse,
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
