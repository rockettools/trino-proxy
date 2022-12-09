module.exports = function (app) {
  require("./cluster")(app);
  require("./trino")(app);
  require("./user")(app);
};
