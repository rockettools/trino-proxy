/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.up = async function (knex) {
  await knex.transaction(async (trx) => {
    await knex.raw("UPDATE query SET status = UPPER(status)").transacting(trx);
    await knex
      .raw("UPDATE cluster SET status = UPPER(status)")
      .transacting(trx);
  });
};

/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.down = async function (knex) {
  await knex.transaction(async (trx) => {
    await knex.raw("UPDATE query SET status = LOWER(status)").transacting(trx);
    await knex
      .raw("UPDATE cluster SET status = LOWER(status)")
      .transacting(trx);
  });
};
