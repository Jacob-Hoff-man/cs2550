#!/usr/bin/env bash
# TODO: order matters here, also think of a better way of handling this
declare -a table_names=("store" "coffee" "carries" "customer" "sale" "promotion" "promotes" "test-user")

for name in "${table_names[@]}"
do
    psql -U $POSTGRES_USER -d $POSTGRES_DB -a -f "/db/table/$name.sql"
done

psql -U $POSTGRES_USER -d $POSTGRES_DB -a -f /db/seed/$SEED_FILE_NAME.sql