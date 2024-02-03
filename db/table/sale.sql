DROP TABLE IF EXISTS sale;

CREATE TABLE
  sale (
    id serial NOT NULL,
    customer_id int NOT NULL,
    coffee_id int NOT NULL,
    store_id int NOT NULL,
    purchased_time timestamp DEFAULT CURRENT_TIMESTAMP,
    quantity int,
    balance float,
    CONSTRAINT pk_sale PRIMARY KEY (id),
    CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customer (id) ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT fk_coffee FOREIGN KEY (coffee_id) REFERENCES coffee (id) ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT fk_store FOREIGN KEY (store_id) REFERENCES store (id) ON UPDATE CASCADE ON DELETE CASCADE
  );