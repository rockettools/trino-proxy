const express = require("express");
const { knex } = require("../lib/knex");
const logger = require("../lib/logger");

const router = express.Router();

router.get("/v1/query/:id", async function (req, res) {
  const { id } = req.params;

  try {
    const query = await knex("query").where({ id }).first();
    if (!query) {
      return res.status(404).json({ error: "Query not found" });
    }

    return res.status(200).json(query);
  } catch (err) {
    logger.error("Error fetching query", err, { id });
    return res.status(500).json({ error: "A system error has occurred" });
  }
});

module.exports = router;
