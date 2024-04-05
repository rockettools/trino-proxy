const uuidv4 = require("uuid").v4;
const zod = require("zod");
const express = require("express");

const { CLUSTER_STATUS } = require("../lib/trino");
const { knex } = require("../lib/knex");
const logger = require("../lib/logger");

const router = express.Router();

router.get("/v1/cluster", async function (req, res) {
  try {
    const clusters = await knex("cluster");
    const items = clusters.map((cluster) => ({
      id: cluster.id,
      name: cluster.name,
      url: cluster.url,
      status: cluster.status,
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

module.exports = router;
