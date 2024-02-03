DROP DOMAIN IF EXISTS store_type CASCADE;

CREATE DOMAIN store_type varchar(7) CONSTRAINT store_type_value CHECK (VALUE IN ('kiosk', 'sitting'));

DROP TABLE IF EXISTS store cascade;

CREATE TABLE
    store (
        id serial NOT NULL,
        name varchar(50) NOT NULL,
        lon float NOT NULL,
        lat float NOT NULL,
        type store_type NOT NULL,
        CONSTRAINT pk PRIMARY KEY (id),
        CONSTRAINT uq UNIQUE (name) DEFERRABLE INITIALLY IMMEDIATE
    );