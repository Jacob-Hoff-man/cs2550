DROP TABLE IF EXISTS coffee;

CREATE TABLE
    coffee (
        id serial NOT NULL,
        name varchar(50) NOT NULL,
        description VARCHAR(250),
        country_of_origin varchar(60),
        intensity int NOT NULL CHECK (
            intensity >= 1
            AND intensity <= 12
        ),
        price float NOT NULL CHECK (price >= 0),
        CONSTRAINT pk_coffee PRIMARY KEY (id)
    );