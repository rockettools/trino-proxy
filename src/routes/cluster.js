const uuidv4 = require("uuid").v4;
const zod = require("zod");
const express = require("express");

const { knex } = require("../lib/knex");
const { CLUSTER_STATUS } = require("../lib/cluster");

const router = express.Router();

router.get("/v1/cluster", async function (req, res) {
  const clusters = await knex("cluster");
  const items = clusters.map((cluster) => ({
    id: cluster.id,
    name: cluster.name,
    url: cluster.url,
    status: cluster.status,
  }));

  return res.status(200).json({ clusters: items });
});

router.post("/v1/cluster", async function (req, res) {
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

  res.status(201).json({ id: clusterId });
});

router.patch("/v1/cluster/:clusterId", async function (req, res) {
  const clusterId = zod.string().uuid().parse(req.params.clusterId);
  const cluster = await knex("cluster").where({ id: clusterId }).first();
  if (!cluster) {
    return res.status(404).json({ error: "Not found" });
  }

  const clusterUpdate = zod
    .object({
      name: zod.string(),
      status: zod.nativeEnum(CLUSTER_STATUS),
      url: zod.string(),
    })
    .strict() // don't allow extra keys
    .safeParse(req.body);
  if (!clusterUpdate.success) {
    return res.status(400).json({ error: clusterUpdate.error });
  }

  await knex("cluster")
    .where({ id: clusterId })
    .update({ ...clusterUpdate.data, updated_at: new Date() });

  return res.status(200).json({ id: clusterId });
});

module.exports = router;
