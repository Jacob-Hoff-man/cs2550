#!/usr/bin/env bash
docker exec -i -t db /scripts/db-query.sh "$1.sql"
