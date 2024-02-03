DROP TABLE IF EXISTS promotion cascade;

CREATE TABLE
    promotion (
        id serial NOT NULL,
        name varchar(60) NOT NULL,
        start_date date NOT NULL,
        end_date date NOT NULL,
        CONSTRAINT pk_promotion PRIMARY KEY (id),
        CONSTRAINT uq_promotion UNIQUE (name) DEFERRABLE INITIALLY IMMEDIATE,
        CONSTRAINT check_valid_dates CHECK (start_date < end_date)
    );