#!/usr/bin/env bash
docker compose down --volumes
docker compose build --no-cache
docker image prune -f
docker compose up -d --remove-orphans
