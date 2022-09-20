const { knex } = require("../lib/knex");
const uuidv4 = require("uuid").v4;
const _ = require("lodash");

module.exports = function (app) {
  app.get("/v1/cluster", async function (req, res) {
    const clusters = await knex("cluster");
    res.json({
      items: clusters.map(function (cluster) {
        return {
          id: cluster.id,
          name: cluster.name,
          status: cluster.status,
        };
      }),
    });
  });

  app.post("/v1/cluster", async function (req, res) {
    const times = new Date();
    await knex("cluster").insert({
      id: uuidv4(),
      name: req.body.name,
      url: req.body.url,
      created_at: times,
      updated_at: times,
      status: req.body.status || "enabled",
    });
    const clusters = await knex("cluster");
    res.json({
      items: clusters.map(function (cluster) {
        return {
          id: cluster.id,
          name: cluster.name,
          status: cluster.status,
        };
      }),
    });
  });

  app.patch("/v1/cluster/:clusterId", async function (req, res) {
    const cluster = await knex("cluster")
      .where({ id: req.params.clusterId })
      .first();
    if (!cluster) {
      return res.status(404).send("Not found");
    }

    await knex("cluster")
      .where({ id: req.params.clusterId })
      .update(
        _.merge(_.pick(req.body, "status", "name"), { updated_at: new Date() })
      );

    res.json({
      status: "done",
    });
  });
};
