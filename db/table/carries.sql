DROP TABLE IF EXISTS carries;

CREATE TABLE
    carries (
        promotion_id int NOT NULL,
        store_id int NOT NULL,
        CONSTRAINT pk_carries PRIMARY KEY (promotion_id, store_id) NOT DEFERRABLE,
        CONSTRAINT fk_promotion FOREIGN KEY (promotion_id) REFERENCES promotion (promotion_id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY IMMEDIATE,
        CONSTRAINT fk_store FOREIGN KEY (store_id) REFERENCES store (store_id) ON UPDATE CASCADE ON DELETE CASCADE DEFERRABLE INITIALLY IMMEDIATE
    );