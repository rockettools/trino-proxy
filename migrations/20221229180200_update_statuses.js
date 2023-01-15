/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.up = async function (knex) {
  await knex.transaction(async (trx) => {
    await knex("query")
      .where("status", "awaiting_scheduling")
      .update({
        status: "AWAITING_SCHEDULING",
      })
      .transacting(trx);

    await knex("query")
      .where("status", "failed")
      .update({
        status: "FAILED",
      })
      .transacting(trx);

    await knex("query")
      .where("status", "finished")
      .update({
        status: "FINISHED",
      })
      .transacting(trx);

    await knex("query")
      .where("status", "lost")
      .update({
        status: "LOST",
      })
      .transacting(trx);

    await knex("query")
      .where("status", "queued")
      .update({
        status: "QUEUED",
      })
      .transacting(trx);
  });
};

/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.down = function () {};
