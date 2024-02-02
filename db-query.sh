#!/usr/bin/env bash
psql -U $POSTGRES_USER -d $POSTGRES_DB -a -f /db/query/$1