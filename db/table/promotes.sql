DROP TABLE IF EXISTS INCLUDES cascade;

CREATE TABLE
    promotes (
        promotion_id int NOT NULL,
        coffee_id int NOT NULL,
        CONSTRAINT pk PRIMARY KEY (promotion_id, coffee_id) DEFERRABLE INITIALLY IMMEDIATE,
        CONSTRAINT fk_coffee FOREIGN KEY (coffee_id) REFERENCES coffee (id) ON UPDATE CASCADE ON DELETE CASCADE,
        CONSTRAINT fk_promotion FOREIGN KEY (promotion_id) REFERENCES promotion (id) ON UPDATE CASCADE ON DELETE CASCADE,
    );