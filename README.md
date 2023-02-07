# Trino Proxy

A load-balancer and gateway service for Trino.

## Running Locally

This project requires Node16+, Docker, and docker-compose. Please ensure you have these tools installed and updated.

Build and run docker containers:

```sh
make clean      # remove all existing containers
make migrate    # create postgres db, run migrations, and seed data
make start      # run trino-proxy
```

In another shell, you can use the Trino CLI to execute commands against the cluster (via trino-proxy):

```sh
brew install trino
trino --server http://localhost:8080 --user admin
```

Creating a new user via API:

```sh
curl \
    --request POST \
    --user "admin" \
    --json '{ "username": "test", "password": "", "tags": ["test"] }' \
    http://localhost:8080/v1/user
```

## Contributing

Format and lint the source code:

```sh
make format
```

## Configuration

- DB_URL: postgres database connection string
- HTTP_ENABLED: whether HTTP server is enabled (set to true)
- HTTP_LISTEN_PORT: HTTP port (default: 8080)
- HTTPS_CERT: HTTPS certificate
- HTTPS_ENABLED: whether HTTPS server is enabled (set to true)
- HTTPS_KEY: HTTPS private key
- HTTPS_LISTEN_PORT: HTTPS port (default: 8443)
- LOG_LEVEL: log level (default: info)
- NODE_ENV: development or production
- STATSD_HOST: statsd agent host (default: localhost)
- STATSD_PORT: statsd agent port (default: 8125)
- STATSD_PREFIX: prefix for statsd metrics (default: trino_proxy)
