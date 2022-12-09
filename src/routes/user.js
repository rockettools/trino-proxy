const argon2 = require("argon2");
const uuidv4 = require("uuid").v4;
const _ = require("lodash");

const { knex } = require("../lib/knex");

module.exports = function (app) {
  app.get("/v1/user", async function (req, res) {
    const users = await knex("user");
    res.json({
      items: users.map(function (user) {
        return { id: user.id, name: user.name };
      }),
    });
  });

  app.get("/v1/user/me", async function (req, res) {
    res.json(req.user);
  });

  app.post("/v1/user", async function (req, res) {
    // pull the user

    if (!req.user) {
      // unless this is the first user, we should block this
      const c = await knex("user").count("*");
      if (c[0].count > 0) {
        return res.status(401).json({ error: "Unauthorized" });
      }
    }

    if (!req.user && !req.body.password) {
      return res.status(400).json({ error: "Invalid input" });
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

    const userId = uuidv4();
    await knex("user").insert({
      id: userId,
      name: req.body.username,
      password: req.body.password,
      created_at: new Date(),
    });

    res.json({ id: userId });
  });

  app.patch("/v1/user/:userId", async function (req, res) {
    // pull the user

    if (!req.user) {
      return res.status(401).json({ error: "Unauthorized" });
    }
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

    res.json({ status: "updated" });
  });
};
