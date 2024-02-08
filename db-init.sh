#!/usr/bin/env bash
# TODO: order matters here, also think of a better way of handling this
declare -a table_names=("store" "coffee" "customer" "carries" "sale" "promotion" "promotes")
for name in "${table_names[@]}"
do
    psql -U $POSTGRES_USER -d $POSTGRES_DB -a -f "/db/table/$name.sql"
done

# TODO: order matters here, also think of a better way of handling this
declare -a view_names=("last-quarter-performance")
for name in "${view_names[@]}"
do
    psql -U $POSTGRES_USER -d $POSTGRES_DB -a -f "/db/view/$name.sql"
done

# TODO: order matters here, also think of a better way of handling this
declare -a func_names=("avg-customer-per-store" "avg-customer-per-store" "customer-coffee-intensity")
for name in "${func_names[@]}"
do
    psql -U $POSTGRES_USER -d $POSTGRES_DB -a -f "/db/func/$name.sql"
done

# TODO: order matters here, also think of a better way of handling this
declare -a proc_names=("monthly-coffee-promotion")
for name in "${proc_names[@]}"
do
    psql -U $POSTGRES_USER -d $POSTGRES_DB -a -f "/db/proc/$name.sql"
done

psql -U $POSTGRES_USER -d $POSTGRES_DB -a -f /db/seed/$SEED_FILE_NAME.sql