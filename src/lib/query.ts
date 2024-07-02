import { knex } from "./knex";
import { traceCache } from "./memcache";
import logger from "./logger";
import stats from "./stats";

import type { Parsers, Query } from "../types/models";

export const QUERY_STATUS = {
  // Trino Proxy states
  AWAITING_SCHEDULING: "AWAITING_SCHEDULING",
  CANCELLED: "CANCELLED",
  RESULT_SET_ROW_LIMIT: "RESULT_SET_ROW_LIMIT",
  MAX_DOWNLOAD_BYTES_LIMIT: "MAX_DOWNLOAD_BYTES_LIMIT",
  NO_VALID_CLUSTERS: "NO_VALID_CLUSTERS",
  // Trino states
  BLOCKED: "BLOCKED",
  FAILED: "FAILED",
  FINISHED: "FINISHED",
  FINISHING: "FINISHING",
  LOST: "LOST",
  PLANNING: "PLANNING",
  QUEUED: "QUEUED",
  RUNNING: "RUNNING",
  STARTING: "STARTING",
} as const;

export async function getQueryById(newQueryId: string) {
  return knex("query").where({ id: newQueryId }).first();
}

export async function getFirstQueryByTraceId(traceId: string) {
  return knex("query")
    .where({ trace_id: traceId })
    .orderBy("created_at", "asc")
    .first();
}

export async function getQueryHeaderInfo(
  traceId: string | undefined
): Promise<{ user: string | null; tags: string[] } | null> {
  if (!traceId) {
    return null;
  }

  const cachedHeaderData = traceCache.get(traceId);
  if (cachedHeaderData) {
    return cachedHeaderData;
  }

  const firstQueryInTrace = await getFirstQueryByTraceId(traceId);
  if (firstQueryInTrace) {
    return {
      user: firstQueryInTrace.assumed_user || firstQueryInTrace.user,
      tags: firstQueryInTrace.tags || [],
    };
  }

  return null;
}

export async function updateQuery(queryId: string, data: Partial<Query> = {}) {
  try {
    await knex("query").where({ id: queryId }).update(data);
  } catch (err) {
    logger.error("Error updating query status", err, { queryId });
  }

  logger.debug("Updated query", { queryId, data });
  stats.increment("query_updated", [`status:${data.status}`]);
}

export function parseFirstQueryHeader(query: string, parsers: Parsers = {}) {
  const parsedInfo = {
    user: null as string | null,
    tags: [] as string[],
  };

  if (parsers?.user) {
    const parsedUser = new RegExp(parsers.user).exec(query);
    if (parsedUser) {
      parsedInfo.user = parsedUser[1];
    }
  }

  if (parsers?.tags) {
    const parsedTags = new RegExp(parsers.tags).exec(query);
    if (parsedTags && parsedTags[1]) {
      const tags = parsedTags[1].split(",");
      parsedInfo.tags.push(...tags);
    }
  }

  return parsedInfo;
}
