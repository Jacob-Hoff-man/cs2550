# CS2550
## Final Project
### How To Run
- Phase 1:
    - [Python must be installed on the host machine to run this project](https://www.python.org/downloads/)

    - use the following command to run the phase 1 script, and then view the results in the `logs/final-project/transaction-manager/` directory:
        ```
            bash run-phase-1.sh
        ```
- Phase 2:
    - TBD

- Phase 3:
    - TBD

## HW 2
### How To Run
- [Docker Engine must be installed on the host machine to run this project](https://docs.docker.com/engine/install/).
    - if using linux, try the `install-docker.sh` script from the root directory:
        ```
            bash install-docker.sh
        ```

- The project is launched using `docker-compose`, which builds a container stack that includes a Postgres server and PgAdmin.
    - Use the `run-seed.sh` script from the root directory to freshly build, launch, and seed the project (THIS WILL DELETE ANY PERSISTED DATABASE CHANGES MADE AFTER THE LAST SEED):
        ```
            bash run-seed.sh
        ```

    - Use the `run-build.sh` script from the root directory to persist the current database, but rebuild the db Docker images (use when db source code has been modified and needs updated, but not seeded):
        ```
            bash run-build.sh
        ```

- After the containers have been spun up and initialized for the first time, the db will have been seeded with data.

- There are queries available in the Postgres environment that can be run.
    -  In the following command, the .sql file name used under the `cs2550/db/query/` dir is `{query-name}`
    - Use the `run-query.sh {query-name}` script from the root directory to run a query:
        ```
            bash run-query.sh test-query test-query2 ...
        ```
