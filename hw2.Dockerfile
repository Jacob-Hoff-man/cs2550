FROM postgres:latest as db

WORKDIR /

COPY ./db ./db
COPY ./hw2/jah292-db.sql ./db
COPY ./hw2/jah292-query.sql ./db
COPY ./db-init-hw2.sh /docker-entrypoint-initdb.d
COPY ./db-query.sh ./scripts/
