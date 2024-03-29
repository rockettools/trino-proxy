const argon2 = require("argon2");

const { knex } = require("../lib/knex");
const logger = require("../lib/logger");

module.exports = async function (req, res, next) {
  let username, password; // X-Trino-User

  if (req.headers["authorization"]) {
    let header = req.headers["authorization"];
    if (typeof header === "string") {
      header = [header];
    }
    for (let idx = 0; idx < header.length; idx++) {
      if (header[idx].indexOf("Basic ") === 0) {
        const foundHeader = Buffer.from(header[idx].split(" ")[1], "base64")
          .toString()
          .split(":");

        username = foundHeader[0];
        password = foundHeader[1];
        logger.silly("Found Auth header", { username });

        // only accept the first Authorization header
        break;
      }
    }
  } else if (req.headers["x-trino-user"]) {
    username = req.headers["x-trino-user"];
    logger.silly("Found Trino User header", { username });
  }

  if (username) {
    const user = await knex("user")
      .where({
        name: username,
      })
      .first();

    if (user) {
      let rightPassword = false;

      if (!password && user.password && user.password.length > 0) {
        return res.status(401).json({ error: "Bad user/password" });
      }

      if (user.password) {
        // check all passwords to allow for password rotation
        for (let idx = 0; idx < user.password.length; idx++) {
          if (await argon2.verify(user.password[idx], password)) {
            rightPassword = true;
          }
        }
      }

      if (!rightPassword && user.password && user.password.length !== 0) {
        return res.status(401).json({ error: "Bad user/password" });
      } else {
        req.user = {
          id: user.id,
          username: username,
          parsers: user.parsers,
          tags: user.tags || [],
        };
      }
    }
  }

  next();
};
