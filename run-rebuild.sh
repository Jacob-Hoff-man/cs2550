#!/usr/bin/env bash
docker compose down --volumes
rm -r -v ./db-pgadmin-data
rm -r -v ./db-postgres-data
docker compose build --no-cache
docker image prune -f
docker compose up -d --remove-orphans
