DROP TABLE IF EXISTS promotes;

CREATE TABLE
    promotes (
        promotion_id int NOT NULL,
        coffee_id int NOT NULL,
        CONSTRAINT pk_promotes PRIMARY KEY (promotion_id, coffee_id) NOT DEFERRABLE,
        CONSTRAINT fk_coffee FOREIGN KEY (coffee_id) REFERENCES coffee (coffee_id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY IMMEDIATE,
        CONSTRAINT fk_promotion FOREIGN KEY (promotion_id) REFERENCES promotion (promotion_id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY IMMEDIATE
    );