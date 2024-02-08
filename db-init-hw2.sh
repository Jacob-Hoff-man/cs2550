#!/usr/bin/env bash
psql -U $POSTGRES_USER -d $POSTGRES_DB -a -f /db/jah292-db.sql
psql -U $POSTGRES_USER -d $POSTGRES_DB -a -f /db/seed/$SEED_FILE_NAME.sql
psql -U $POSTGRES_USER -d $POSTGRES_DB -a -f /db/jah292-query.sql