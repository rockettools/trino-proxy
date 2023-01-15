const express = require("express");
const argon2 = require("argon2");
const uuidv4 = require("uuid").v4;
const _ = require("lodash");

const { knex } = require("../lib/knex");
const logger = require("../lib/logger");

const router = express.Router();

router.get("/v1/user", async function (_req, res) {
  try {
    const data = await knex("user");
    const users = data.map((user) => ({
      id: user.id,
      name: user.name,
      parsers: user.parsers,
      tags: user.tags,
    }));

    return res.status(200).json({ users });
  } catch (err) {
    logger.error("Error fetching users", err);
    return res.status(500).json({ error: "A system error has occured" });
  }
});

router.get("/v1/user/me", async function (req, res) {
  if (!req.user) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  return res.status(200).json(req.user);
});

router.post("/v1/user", async function (req, res) {
  try {
    if (!req.user) {
      // unless this is the first user, we should block this
      const c = await knex("user").count("*");
      if (c[0].count > 0) {
        return res.status(401).json({ error: "Unauthorized" });
      }
    }

    const { username, password, parsers = null, tags = [] } = req.body;
    const dbPasswordList = [];
    if (!req.user && !password) {
      return res.status(400).json({ error: "Invalid input" });
    }

    if (password) {
      const passwordList = typeof password === "string" ? [password] : password;
      for (let idx = 0; idx < passwordList.length; idx++) {
        dbPasswordList.push(await argon2.hash(passwordList[idx]));
      }
    }

    const userId = uuidv4();
    await knex("user").insert({
      id: userId,
      name: username,
      password: dbPasswordList,
      parsers,
      tags,
      created_at: new Date(),
    });

    return res.status(200).json({ id: userId });
  } catch (err) {
    logger.error("Error creating new user", err);
    return res.status(500).json({ error: "A system error has occured" });
  }
});

router.patch("/v1/user/:userId", async function (req, res) {
  if (!req.user) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  try {
    const user = await knex("user").where({ id: req.params.userId }).first();
    if (!user) {
      return res.status(404).json({ error: "Not found" });
    }

    if (req.body.password) {
      if (typeof req.body.password === "string") {
        req.body.password = [req.body.password];
      }
      let hashedPasswords = [];
      for (let idx = 0; idx < req.body.password.length; idx++) {
        hashedPasswords.push(await argon2.hash(req.body.password[idx]));
      }
      req.body.password = hashedPasswords;
    }

    await knex("user")
      .where({ id: req.params.userId })
      .update(
        _.merge(_.pick(req.body, "parsers", "password"), {
          updated_at: new Date(),
        })
      );

    return res.status(200).json({ status: "updated" });
  } catch (err) {
    logger.error("Error updating user", err);
    return res.status(500).json({ error: "A system error has occured" });
  }
});

module.exports = router;
