FROM node:16-buster AS builder
WORKDIR  /app
COPY package.json yarn.lock ./
RUN yarn install --frozen-lockfile --production

# Distroless production image
FROM gcr.io/distroless/nodejs:16
WORKDIR /app
COPY --from=builder /app/node_modules /app/node_modules
COPY src /app/src
COPY migrations /app/migrations
COPY package.json knexfile.js /app/

EXPOSE 8080
CMD ["/app/src/server.js"]
