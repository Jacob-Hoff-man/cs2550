#!/usr/bin/env bash
docker compose down --volumes
rm -r ./db-pgadmin-data
rm -r ./db-postgres-data
docker compose build --no-cache
docker image prune -f
docker compose up -d --remove-orphans
