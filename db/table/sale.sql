DROP TABLE IF EXISTS sale CASCADE;

CREATE TABLE
  sale (
    id serial NOT NULL,
    customer_id int NOT NULL,
    purchased_time timestamp DEFAULT CURRENT_TIMESTAMP,
    balance float,
    CONSTRAINT pk PRIMARY KEY (id),
    CONSTRAINT fk FOREIGN KEY (customer_id) REFERENCES customer (id) ON UPDATE CASCADE ON DELETE CASCADE
  );