const argon2 = require("argon2");
const { knex } = require("../lib/knex");
const uuidv4 = require("uuid").v4;
const _ = require("lodash");

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
      console.log("c", c);
      if (c[0].count > 0) {
        return res.status(401).send("Unauthorized");
      }
    }
    if (!req.body.password) {
      return res.status(400).send("Invalid input.");
    }
    const hash = await argon2.hash(req.body.password);

    await knex("user").insert({
      id: uuidv4(),
      name: req.body.username,
      password: [hash],
      created_at: new Date(),
    });

    res.json({ hash });
  });
  app.patch("/v1/user/:userId", async function (req, res) {
    // pull the user

    if (!req.user) {
      return res.status(401).send("Unauthorized");
    }
    const user = await knex("user").where({ id: req.params.userId }).first();

    if (!user) {
      return res.status(404).send("not found");
    }

    await knex("user")
      .where({ id: req.params.userId })
      .update(_.merge(_.pick(req.body, "parsers"), { updated_at: new Date() }));

    res.json({ status: "updated" });
  });
};
