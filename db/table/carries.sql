DROP TABLE IF EXISTS carries;

CREATE TABLE
    carries (
        coffee_id int NOT NULL,
        store_id int NOT NULL,
        CONSTRAINT pk_carries PRIMARY KEY (coffee_id, store_id) NOT DEFERRABLE,
        CONSTRAINT fk_coffee FOREIGN KEY (coffee_id) REFERENCES coffee (coffee_id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY IMMEDIATE,
        CONSTRAINT fk_store FOREIGN KEY (store_id) REFERENCES store (store_id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY IMMEDIATE
    );