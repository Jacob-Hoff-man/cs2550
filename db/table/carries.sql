DROP TABLE IF EXISTS carries;

CREATE TABLE
    carries (
        promotion_id int NOT NULL,
        store_id int NOT NULL,
        CONSTRAINT pk_carries PRIMARY KEY (promotion_id, store_id) DEFERRABLE INITIALLY IMMEDIATE,
        CONSTRAINT fk_promotion FOREIGN KEY (promotion_id) REFERENCES store (id) ON UPDATE CASCADE ON DELETE CASCADE,
        CONSTRAINT fk_store FOREIGN KEY (store_id) REFERENCES store (id) ON UPDATE CASCADE ON DELETE CASCADE
    );