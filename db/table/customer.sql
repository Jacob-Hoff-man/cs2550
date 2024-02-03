DROP DOMAIN IF EXISTS month_enum;

CREATE DOMAIN month_enum AS char(3) CONSTRAINT month_enum_value CHECK (
    VALUE in (
        'Jan',
        'Feb',
        'Mar',
        'Apr',
        'May',
        'Jun',
        'Jul',
        'Aug',
        'Sep',
        'Oct',
        'Nov',
        'Dec'
    )
);

DROP TABLE IF EXISTS customer;

CREATE TABLE
    customer (
        customer_id serial NOT NULL,
        fname varchar(60) NOT NULL,
        lname varchar(60) NOT NULL,
        email varchar(60) NOT NULL,
        month_of_birth month_enum NOT NULL,
        day_of_birth char(2) NOT NULL,
        CONSTRAINT pk_customer PRIMARY KEY (customer_id) NOT DEFERRABLE,
        CONSTRAINT uq_customer UNIQUE (email) DEFERRABLE INITIALLY DEFERRED
    );