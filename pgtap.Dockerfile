FROM postgres:latest as db

WORKDIR /

COPY ./db ./db
COPY ./db-init.sh /docker-entrypoint-initdb.d
COPY ./db-query.sh ./scripts/

RUN apt-get update \
    && apt-get install -y build-essential git-core libv8-dev curl postgresql-server-dev-$PG_MAJOR \
    && rm -rf /var/lib/apt/lists/*

# install pg_prove
RUN curl -LO http://xrl.us/cpanm \
    && chmod +x cpanm \
    && ./cpanm TAP::Parser::SourceHandler::pgTAP

# install pgtap
RUN git clone https://github.com/theory/pgtap.git \
    && cd pgtap \
    && make \
    && make install

