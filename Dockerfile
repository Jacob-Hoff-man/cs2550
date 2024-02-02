FROM postgres:latest as db

WORKDIR /

COPY ./db ./db
COPY ./db-init.sh /docker-entrypoint-initdb.d
COPY ./db-query.sh ./scripts/


