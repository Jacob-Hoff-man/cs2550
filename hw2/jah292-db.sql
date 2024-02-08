-- Jacob Hoffman
-- Jah292

-- Q2

-- STORE
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
    
-- COFFEE
DROP TABLE IF EXISTS coffee;

CREATE TABLE
    coffee (
        coffee_id serial NOT NULL,
        name varchar(60) NOT NULL,
        description VARCHAR(280),
        country_of_origin varchar(60),
        intensity int NOT NULL CHECK (
            intensity >= 1
            AND intensity <= 12
        ),
        price float NOT NULL CHECK (price >= 0),
        CONSTRAINT pk_coffee PRIMARY KEY (coffee_id) NOT DEFERRABLE
    );

-- CUSTOMER
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

-- CARRIES
DROP TABLE IF EXISTS carries;

CREATE TABLE
    carries (
        coffee_id int NOT NULL,
        store_id int NOT NULL,
        inventory int DEFAULT 200,
        inventory_day char(2) DEFAULT EXTRACT(DAY FROM CURRENT_DATE),
        CONSTRAINT pk_carries PRIMARY KEY (coffee_id, store_id) NOT DEFERRABLE,
        CONSTRAINT fk_coffee FOREIGN KEY (coffee_id) REFERENCES coffee (coffee_id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY IMMEDIATE,
        CONSTRAINT fk_store FOREIGN KEY (store_id) REFERENCES store (store_id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY IMMEDIATE
    );

-- SALE
DROP TABLE IF EXISTS sale;

CREATE TABLE
  sale (
    sale_id serial NOT NULL,
    customer_id int NOT NULL,
    store_id int NOT NULL,
    coffee_id int NOT NULL,
    quantity int,
    time_of_purchase timestamp DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_sale PRIMARY KEY (sale_id) NOT DEFERRABLE,
    CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customer (customer_id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT fk_coffee FOREIGN KEY (coffee_id) REFERENCES coffee (coffee_id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY IMMEDIATE,
    CONSTRAINT fk_store FOREIGN KEY (store_id) REFERENCES store (store_id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY IMMEDIATE
  );

-- PROMOTION
DROP TABLE IF EXISTS promotion;

CREATE TABLE
    promotion (
        promotion_id serial NOT NULL,
        name varchar(60) NOT NULL,
        start_date date NOT NULL,
        end_date date NOT NULL,
        CONSTRAINT pk_promotion PRIMARY KEY (promotion_id) NOT DEFERRABLE,
        CONSTRAINT check_valid_dates CHECK (start_date < end_date)
    );

-- PROMOTES
DROP TABLE IF EXISTS promotes;

CREATE TABLE
    promotes (
        promotion_id int NOT NULL,
        coffee_id int NOT NULL,
        CONSTRAINT pk_promotes PRIMARY KEY (promotion_id, coffee_id) NOT DEFERRABLE,
        CONSTRAINT fk_coffee FOREIGN KEY (coffee_id) REFERENCES coffee (coffee_id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY IMMEDIATE,
        CONSTRAINT fk_promotion FOREIGN KEY (promotion_id) REFERENCES promotion (promotion_id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY IMMEDIATE
    );