# Install dependencies using lockfile
install:
	yarn install --frozen-lockfile

# Update dependencies in lockfile
install-update:
	yarn install

# Format and lint source code
format:
	yarn format
	yarn lint

# Start docker containers
start:
	docker-compose up --build --attach api --attach scheduler

# Run database migrations against db
migrate:
	docker-compose up -d postgres
	docker-compose run --build api /app/node_modules/.bin/knex migrate:latest
	docker-compose run --build api /app/node_modules/.bin/knex seed:run

# Cleanup all docker containers
clean:
	docker-compose down --rmi local --volumes --remove-orphans

reset: clean migrate start
