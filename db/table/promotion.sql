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