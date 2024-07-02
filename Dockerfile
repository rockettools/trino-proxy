FROM node:18-buster AS builder
WORKDIR  /app
COPY package.json yarn.lock tsconfig.json ./
RUN yarn install --frozen-lockfile
COPY src /app/src
RUN yarn build

FROM node:18-buster AS dependencies
WORKDIR  /app
COPY package.json yarn.lock ./
RUN yarn install --frozen-lockfile --production

# Distroless production image
FROM gcr.io/distroless/nodejs:18
WORKDIR /app
COPY --from=builder /app/dist /app/dist
COPY --from=dependencies /app/node_modules /app/node_modules
COPY migrations /app/migrations
COPY package.json knexfile.js /app/

EXPOSE 8080
CMD ["/app/dist/server.js"]
