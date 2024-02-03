DROP DOMAIN IF EXISTS month_enum CASCADE;

CREATE DOMAIN month_enum AS char(3) CONSTRAINT month_enum_value CHECK (
    VALUE in (
        'jan',
        'feb',
        'mar',
        'apr',
        'may',
        'jun',
        'jul',
        'aug',
        'sep',
        'oct',
        'nov',
        'dec'
    )
);

DROP TABLE IF EXISTS customer CASCADE;

CREATE TABLE
    customer (
        id serial NOT NULL,
        first_name varchar(50) NOT NULL,
        last_name varchar(50) NOT NULL,
        birth_month month_enum NOT NULL,
        birth_day char(2) NOT NULL,
        email varchar(32) NOT NULL,
        CONSTRAINT pk PRIMARY KEY (id),
        CONSTRAINT uq UNIQUE (email) DEFERRABLE INITIALLY IMMEDIATE,
    );