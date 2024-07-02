import express from "express";
import argon2 from "argon2";
import { v4 as uuidv4 } from "uuid";

import { knex } from "../lib/knex";
import logger from "../lib/logger";

import type { Request, Response } from "express";

const router = express.Router();

async function getHashedPasswords(password?: string) {
  if (!password) return [];

  // Password input can be a single value, or a list of values
  const hashedPasswords = [];
  const passwordList = typeof password === "string" ? [password] : password;
  for (const rawPassword of passwordList) {
    const hashedPassword = await argon2.hash(rawPassword);
    hashedPasswords.push(hashedPassword);
  }

  return hashedPasswords;
}

router.get("/v1/user", async function (req: Request, res: Response) {
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
    return res.status(500).json({ error: "A system error has occurred" });
  }
});

router.get("/v1/user/me", async function (req: Request, res: Response) {
  if (!req.user) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  return res.status(200).json(req.user);
});

router.post("/v1/user", async function (req: Request, res: Response) {
  const { username, password, parsers = null, tags = [] } = req.body;

  try {
    // Allow the first user to be created without middleware authentication
    if (!req.user) {
      // unless this is the first user, we should block this
      const c = await knex("user").count("*");
      if (Number(c[0].count) > 0) {
        return res.status(401).json({ error: "Unauthorized" });
      }

      // First user has to have a password for security purposes
      // First user also has to be an admin, otherwise no other users can be created
      if (!password || !tags.includes("admin")) {
        return res.status(400).json({ error: "Invalid input" });
      }
    }

    // If it's not the first user, the user must be an admin
    if (req.user && !req.user.tags.includes("admin")) {
      return res.status(401).json({ error: "Unauthorized" });
    }

    const userId = uuidv4();
    const passwordList = await getHashedPasswords(password);
    await knex("user").insert({
      id: userId,
      name: username,
      password: passwordList,
      parsers,
      tags,
      created_at: new Date(),
    });

    return res.status(200).json({ id: userId });
  } catch (err) {
    logger.error("Error creating new user", err);
    return res.status(500).json({ error: "A system error has occurred" });
  }
});

router.patch("/v1/user/:userId", async function (req: Request, res: Response) {
  // Only admin users can adjust tags
  // Otherwise, all users can make themselves an admin
  if (!req.user || !req.user.tags.includes("admin")) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  try {
    const userId = req.params.userId;
    const user = await knex("user").where({ id: userId }).first();
    if (!user) {
      return res.status(404).json({ error: "Not found" });
    }

    const { password, parsers = null, tags = [] } = req.body;
    const passwordList = await getHashedPasswords(password);

    await knex("user").where({ id: userId }).update({
      password: passwordList,
      parsers,
      tags,
      updated_at: new Date(),
    });

    return res.status(200).json({ id: userId });
  } catch (err) {
    logger.error("Error updating user", err);
    return res.status(500).json({ error: "A system error has occurred" });
  }
});

export default router;
