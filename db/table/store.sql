DROP DOMAIN IF EXISTS store_type;

CREATE DOMAIN store_type varchar(13) CONSTRAINT store_type_value CHECK (VALUE IN ('kiosk', 'sitting', 'drive-through'));

DROP TABLE IF EXISTS store;

CREATE TABLE
    store (
        store_id serial NOT NULL,
        name varchar(60) NOT NULL,
        type store_type NOT NULL,
        gps_lon decimal(7, 2) NOT NULL,
        gps_lat decimal(7, 2) NOT NULL,
        CONSTRAINT pk_store PRIMARY KEY (store_id),
        CONSTRAINT uq_store UNIQUE (name) DEFERRABLE INITIALLY DEFERRED
    );