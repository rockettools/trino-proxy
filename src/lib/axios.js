const axios = require("axios").default;
const axiosRetry = require("axios-retry");

const axiosClient = axios.create();
axiosRetry(axiosClient, {
  retries: 3,
  retryDelay: (retryCount) => retryCount * 1000,
  retryCondition: (error) => {
    const statusCode = error.response.status;
    return statusCode === 500 || statusCode === 503;
  },
});

module.exports = {
  axios: axiosClient,
};
