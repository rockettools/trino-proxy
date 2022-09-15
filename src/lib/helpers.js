const { client } = require("./redis");

exports.updateUrls = function updateUrls(body, newQueryId, host) {
  const queryId = body.id;
  body.id = newQueryId;

  if (body.infoUri) {
    body.infoUri = body.infoUri
      .replace(queryId, newQueryId)
      .replace(/https?:\/\/[^/]+/, host);
  }
  if (body.nextUri) {
    body.nextUri = body.nextUri
      .replace(queryId, newQueryId)
      .replace(/https?:\/\/[^/]+/, host);
  }
  return body;
};

exports.getUsernameFromAuthorizationHeader =
  function getUsernameFromAuthorizationHeader(header) {
    if (header) {
      if (typeof header === "string") {
        header = [header];
      }
      for (let idx = 0; idx < header.length; idx++) {
        if (header[idx].indexOf("Basic ") === 0)
          return Buffer.from(header[idx].split(" ")[1], "base64").toString();
      }
    }
  };

exports.replaceAuthorizationHeader = async function replaceAuthorizationHeader(
  req
) {
  let headerUser;
  if (req.headers["x-trino-trace-token"]) {
    const info = await client.get(req.headers["x-trino-trace-token"]);
    console.log("Trace: " + req.headers["x-trino-trace-token"], info);

    // If this is the first query in the sequence it should have the header, try and parse.
    if (info) {
      headerUser = info;
    }
  }

  if (headerUser) {
    console.log("Replacing with: " + headerUser);
    req.headers.authorization =
      "Basic " + Buffer.from(headerUser).toString("base64");
  }
};
