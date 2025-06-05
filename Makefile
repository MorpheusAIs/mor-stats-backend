include .env
export
DOCKER_CMD := $(shell command -v docker 2> /dev/null || echo podman) # handle using docker or podman

install:
	pip install -r requirements.txt

build:
	$(DOCKER_CMD) build .

up:
	$(DOCKER_CMD) compose -f ./docker/docker-compose.yml up

up-force-build:
	$(DOCKER_CMD) compose -f ./docker/docker-compose.yml up --build

db-up:
	$(DOCKER_CMD) compose -f ./docker/docker-compose.yml start postgres

down:
	$(DOCKER_CMD) compose -f ./docker/docker-compose.yml down

# Seed the local database
seed-local:
	python scripts/seed_database.py

# Seed the Docker database
seed-docker:
	$(DOCKER_CMD) compose -f ./docker/docker-compose.yml exec -T postgres psql -U $(DB_USER) -d $(DB_NAME) -c "SELECT 1" > /dev/null 2>&1 || (echo "PostgreSQL is not running. Starting it..." && $(DOCKER_CMD) compose -f ./docker/docker-compose.yml start postgres && sleep 5)
	$(DOCKER_CMD) compose -f ./docker/docker-compose.yml exec -T mor-stats python seed_database.py

seed-docker-circulating:
	$(DOCKER_CMD) compose -f ./docker/docker-compose.yml exec -T postgres psql -U $(DB_USER) -d $(DB_NAME) -c "SELECT 1" > /dev/null 2>&1 || (echo "PostgreSQL is not running. Starting it..." && $(DOCKER_CMD) compose -f ./docker/docker-compose.yml start postgres && sleep 5)
	$(DOCKER_CMD) compose -f ./docker/docker-compose.yml exec -T mor-stats python seed_database.py --seedcirculating
