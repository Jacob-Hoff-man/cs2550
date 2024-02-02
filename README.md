# CS2550

### How To Run
- [Docker Engine must be installed on the host machine to run this project](https://docs.docker.com/engine/install/).
    - if using linux, try the `install-docker.sh` script from the root directory:
        ```
            bash install-docker.sh
        ```

- The project is launched using `docker-compose`, which builds a container stack that includes a Postgres server and Adminer.
    - Use the `run-rebuild.sh` script from the root directory to launch the project:
        ```
            bash run-rebuild.sh
        ```

- After the containers have been spun up and initialized, the db will have been seeded with data.
- There are queries available in the Postgres environment, which can be run.
    - Use the `run-query.sh {query-name}` script from the root directory to run a query:
        ```
            bash run-query.sh test-query
        ```
