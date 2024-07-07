import argon2 from "argon2";

import { knex } from "../lib/knex";
import logger from "../lib/logger";

import type { NextFunction, Request, Response } from "express";

export default async function (
  req: Request,
  res: Response,
  next: NextFunction
) {
  let username: string | null = null;
  let password: string | null = null;

  const authHeader = req.headers["authorization"];
  const trinoUserHeader = req.headers["x-trino-user"];
  if (authHeader) {
    const header = typeof authHeader === "string" ? [authHeader] : authHeader;

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
  } else if (trinoUserHeader && typeof trinoUserHeader === "string") {
    username = trinoUserHeader;
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

      if (user.password && password) {
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
}
