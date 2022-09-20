module.exports = function (app) {
  require("./routes/cluster")(app);
  require("./routes/trino")(app);
  require("./routes/user")(app);
};
