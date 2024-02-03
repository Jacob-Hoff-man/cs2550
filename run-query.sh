#!/usr/bin/env bash
for var in "$@"
do
    docker exec -i -t db /scripts/db-query.sh "$var.sql"
done
