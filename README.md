# Trino Proxy

A load-balancer and gateway service for Trino.

## Running Locally

This project requires Node16+, Docker, and docker-compose. Please ensure you have these tools installed and updated.

Build and run docker containers:

```sh
make migrate    # create postgres db and run migrations
make start      # run trino-proxy
```

## Contributing

Format and lint the source code:

```sh
make format
```

## Configuration

- BABYSITTER_DELAY: interval for babysitter worker (default: 3 seconds)
- BABYSITTER_DISABLED: disable the query babysitter (set to true)
- DB_URL: postgres database connection string
- HTTP_ENABLED: whether HTTP server is enabled (set to true)
- HTTP_LISTEN_PORT: HTTP port (default: 8080)
- HTTPS_CERT: HTTPS certificate
- HTTPS_ENABLED: whether HTTPS server is enabled (set to true)
- HTTPS_KEY: HTTPS private key
- HTTPS_LISTEN_PORT: HTTPS port (default: 8443)
- LOG_LEVEL: log level (default: info)
- LOG_QUERY: whether queries are logged
- NODE_ENV: development or production
- STATSD_HOST: statsd agent host (default: localhost)
- STATSD_PORT: statsd agent port (default: 8125)
- STATSD_PREFIX: prefix for statsd metrics (default: trino_proxy)
