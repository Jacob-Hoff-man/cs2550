#!/usr/bin/env bash
docker compose down --volumes
docker compose build --no-cache
docker compose up -d
