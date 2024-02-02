#!/usr/bin/env bash
psql -U $POSTGRES_USER -d $POSTGRES_DB -a -f /db/table/user.sql
psql -U $POSTGRES_USER -d $POSTGRES_DB -a -f /db/seed.sql