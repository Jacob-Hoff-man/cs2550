DROP DOMAIN IF EXISTS month_enum;

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

DROP TABLE IF EXISTS customer;

CREATE TABLE
    customer (
        id serial NOT NULL,
        first_name varchar(60) NOT NULL,
        last_name varchar(60) NOT NULL,
        birth_month month_enum NOT NULL,
        birth_day char(2) NOT NULL,
        email varchar(60) NOT NULL,
        CONSTRAINT pk_customer PRIMARY KEY (id) NOT DEFERRABLE,
        CONSTRAINT uq_customer UNIQUE (email) DEFERRABLE INITIALLY DEFERRED
    );