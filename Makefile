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

seed:
	python scripts/seed_database.py
