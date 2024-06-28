import { v4 as uuidv4 } from "uuid";
import zod from "zod";
import express from "express";

import { CLUSTER_STATUS } from "../lib/trino";
import { knex } from "../lib/knex";
import logger from "../lib/logger";

const router = express.Router();

router.get("/v1/cluster", async function (req, res) {
  try {
    const clusters = await knex("cluster");
    const items = clusters.map((cluster) => ({
      id: cluster.id,
      name: cluster.name,
      url: cluster.url,
      status: cluster.status,
      tags: cluster.tags,
    }));

    return res.status(200).json({ clusters: items });
  } catch (err) {
    logger.error("Error fetching clusters", err);
    return res.status(500).json({ error: "A system error has occurred" });
  }
});

router.post("/v1/cluster", async function (req, res) {
  try {
    const times = new Date();
    const newCluster = zod
      .object({
        name: zod.string(),
        url: zod.string(),
        status: zod.nativeEnum(CLUSTER_STATUS),
        tags: zod.string().array(),
      })
      .strict() // don't allow extra keys
      .safeParse(req.body);

    if (!newCluster.success) {
      return res.status(400).json({ error: newCluster.error });
    }

    const clusterId = uuidv4();
    await knex("cluster").insert({
      ...newCluster.data,
      id: clusterId,
      created_at: times,
      updated_at: times,
    });

    return res.status(201).json({ id: clusterId });
  } catch (err) {
    logger.error("Error creating cluster", err);
    return res.status(500).json({ error: "A system error has occurred" });
  }
});

router.patch("/v1/cluster/:clusterId", async function (req, res) {
  try {
    const clusterId = zod.string().uuid().parse(req.params.clusterId);
    const cluster = await knex("cluster").where({ id: clusterId }).first();
    if (!cluster) {
      return res.status(404).json({ error: "Not found" });
    }

    const reqParse = zod
      .object({
        name: zod.string(),
        status: zod.nativeEnum(CLUSTER_STATUS),
        url: zod.string(),
        tags: zod.string().array(),
      })
      .partial() // Make all keys optional
      .strip() // Strip out any extra keys
      .safeParse(req.body);
    if (!reqParse.success) {
      return res.status(400).json({ error: reqParse.error });
    }

    const clusterUpdate = {
      ...cluster,
      ...reqParse.data,
    };

    await knex("cluster")
      .where({ id: clusterId })
      .update({ ...clusterUpdate, updated_at: new Date() });

    return res.status(200).json({ id: clusterId });
  } catch (err) {
    logger.error("Error updating cluster", err);
    return res.status(500).json({ error: "A system error has occurred" });
  }
});

export default router;
