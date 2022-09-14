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
