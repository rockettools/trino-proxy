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
	docker-compose up --build

# Cleanup all docker containers
clean:
	docker-compose down --rmi local --volumes
