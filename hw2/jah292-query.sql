-- Jacob Hoffman
-- Jah292

-- List all tables
SELECT * FROM store;
SELECT * FROM coffee;
SELECT * FROM customer;
SELECT * FROM carries;
SELECT * FROM sale;
SELECT * FROM promotion;
SELECT * FROM promotes;

-- HW2 - Q3A
-- List the first name, last name, emails, and number of purchases of all customers who
-- have made more than one coffee purchase. List them in a descending order based
-- on the number of purchases.
SELECT
    fname,
    lname,
    email,
    COUNT(coffee_id) AS coffee_count
FROM
    CUSTOMER C
    NATURAL JOIN SALE S
WHERE
    C.customer_id = S.customer_id
GROUP BY
    customer_id
ORDER BY
    coffee_count DESC;

-- HW2 - Q3B
-- List the store id, store name, store type, and number of promotions of the store(s)
-- who carried the highest number of promotions. List them in an ascending order of
-- their store name.
SELECT
    store_id,
    name,
    type,
    promotion_count
FROM
    store S
    NATURAL JOIN (
        SELECT
            store_id,
            COUNT(promotion_id) AS promotion_count
        FROM
            carries C
            NATURAL JOIN promotes P
        WHERE
            C.coffee_id = P.coffee_id
        GROUP BY
            store_id
    ) PC
WHERE
    S.store_id = PC.store_id
ORDER BY
    promotion_count DESC;

-- HW2 - Q3C
-- List the store id, store name and store type of all stores of CoffeeDB along with the
-- year in which the store sold the highest total quantity of coffee.
SELECT
    store_id,
    name,
    type,
    year,
    quantity_year_sum
FROM
    store S
    NATURAL JOIN (
        SELECT
            store_id,
            EXTRACT(
                year
                FROM
                    time_of_purchase
            ) AS year,
            SUM(quantity) as quantity_year_sum
        FROM
            sale
        GROUP BY
            store_id,
            year
    ) YS
WHERE
    S.store_id = YS.store_id
    AND (YS.store_id, YS.quantity_year_sum) IN (
        SELECT
            store_id,
            MAX(quantity_year_sum) as max_year_sum
        FROM
            (
                SELECT
                    store_id,
                    EXTRACT(
                        year
                        FROM
                            time_of_purchase
                    ) AS year,
                    SUM(quantity) as quantity_year_sum
                FROM
                    sale
                GROUP BY
                    store_id,
                    year
            )
        GROUP BY
            store_id
        ORDER BY
            store_id ASC
    )
ORDER BY
    store_id ASC;

-- HW2 - Q3D
-- Rank coffees based on the most quantity of the coffee that was sold by CoffeeDB.
-- List: coffee id, coffee name, intensity, and rank.
SELECT
    coffee_id,
    name,
    intensity,
    RANK() OVER (
        ORDER BY
            total_quantity DESC
    ) coffee_sold_rank
FROM
    coffee C
    NATURAL JOIN (
        SELECT
            coffee_id,
            SUM(quantity) AS total_quantity
        FROM
            sale
        GROUP BY
            coffee_id
    ) TQ
WHERE
    C.coffee_id = TQ.coffee_id;

-- HW2 - Q3E
-- List the top-3 customers with the highest number of coffee purchases. List cus-
-- tomer id, fname, lname, email, and number of coffee purchases.
SELECT
    customer_id,
    fname,
    lname,
    email,
    sum_coffee
FROM
    customer C
    NATURAL JOIN (
        SELECT
            customer_id,
            SUM(quantity) as sum_coffee
        FROM
            sale
        GROUP BY
            customer_id
    ) SQ
WHERE
    C.customer_id = SQ.customer_id
ORDER BY
    sum_coffee DESC
LIMIT
    3;

-- HW2 - Q4
-- Create a view named LAST QUARTER PERFORMACE that lists the num-
-- ber of customers that each store sold coffee to during the fourth quarter of 2023 (i.e.,
-- September 1st, 2023 to December 31, 2023). The view schema is: store id, store name,
-- store type, and number of customers.
CREATE VIEW last_quarter_performance AS
SELECT
    store_id,
    name,
    type,
    COUNT(DISTINCT customer_id) AS num_customer
FROM
    sale S NATURAL
    JOIN store ST
WHERE
    S.time_of_purchase BETWEEN '2023-09-01'
    AND '2023-12-31'
    AND S.store_id = ST.store_id
GROUP BY
    store_id,
    name,
    type;

-- HW2 - Q5
-- calculates and returns the average number of customers that 
-- purchased coffee across all stores. The function has no input.
CREATE
OR REPLACE FUNCTION avg_customer_per_store() RETURNS float AS $$ DECLARE avg_customer float;

BEGIN
SELECT
    AVG(num_customer) INTO avg_customer
FROM
    last_quarter_performance;
RETURN avg_customer;

END;

$$ LANGUAGE plpgsql;

-- HW2 - Q6
-- calculates and returns the average number of customers 
-- that purchased coffee across all kiosk stores. The function has no input.
CREATE
OR REPLACE FUNCTION avg_customer_per_kiosk_store() RETURNS float AS $$ DECLARE avg_customer float;

BEGIN
SELECT
    AVG(num_customer) INTO avg_customer
FROM
    last_quarter_performance LQP
WHERE LQP.type = 'kiosk';
RETURN avg_customer;

END;

$$ LANGUAGE plpgsql;

-- HW2 - Q7
-- finds out whether their most recent coffee purchase had a strong coffee profile
-- (intensity greater than 6) or not.
-- The function inputs the customer to query: the id of the customer to be looked up.
-- The function should output a boolean value of true if the most recent coffee purchase
-- had a strong coffee profile and false otherwise.
CREATE
OR REPLACE FUNCTION customer_coffee_intensity(inp_customer_id integer)
RETURNS boolean AS $$ DECLARE customer_latest_intensity integer;

BEGIN
SELECT
    intensity INTO customer_latest_intensity
FROM
    sale S NATURAL
    JOIN coffee C
WHERE
    S.coffee_id = C.coffee_id
    AND S.customer_id = inp_customer_id
ORDER BY
    time_of_purchase DESC
LIMIT
    1;

RETURN customer_latest_intensity > 6;

END;

$$ LANGUAGE plpgsql;

-- HW2 - Q8
-- creates a promotion with the start date set to the day of the procedure call 
-- with the end date set to 30 days after the day of the procedure call. 
-- In addition, the procedure should promote a specific coffee. 
-- The procedure has the following input: coffee_id, promotion_id, promotion_name
CREATE
OR REPLACE PROCEDURE monthly_coffee_promotion_proc(
    inp_coffee_id integer,
    inp_promotion_id integer,
    inp_promotion_name varchar(60)
) LANGUAGE plpgsql AS $$ BEGIN

INSERT INTO
    promotion
    (
        promotion_id,
        name,
        start_date,
        end_date
    )
VALUES
    (
        inp_promotion_id,
        inp_promotion_name,
        CURRENT_DATE,
        CURRENT_DATE + 30
    );

INSERT INTO
    promotes
    (
        promotion_id,
        coffee_id
    )
VALUES
    (
        inp_promotion_id,
        inp_coffee_id
    );

END;

$$;

-- HW2 - Q9 View
CREATE VIEW coffee_inventory AS
SELECT
    store_id,
    coffee_id,
    inventory,
    inventory_day
FROM
    coffee CF NATURAL JOIN carries CA
WHERE
    CF.coffee_id = CA.coffee_id;

-- HW2 - Q9
-- Create a trigger that enforces a semantic 
-- integrity constraint. That is when a store goes to sell a coffee
-- (i.e., a new sale is added to the SALE table), the trigger checks
-- whether the store’s remaining daily inventory is enough to handle the
-- current sale. We will assume that the inventory size for each type of
-- coffee is 200, which is reset on a daily basis. If the store does not have enough
-- remaining quantity of a coffee (i.e., the store has already sold 200 cups of
-- coffee #7 or the new SALE quantity plus prior purchases would exceed 200 cups
-- of coffee #7), the trigger should raise an exception.
CREATE OR REPLACE FUNCTION check_inventory()
RETURNS trigger
AS
$$ DECLARE current_inventory integer; current_inventory_day char(2); current_day char(2);
BEGIN
    current_day := EXTRACT(DAY FROM CURRENT_DATE);
    SELECT inventory INTO current_inventory
    FROM coffee_inventory CI
    WHERE CI.store_id = NEW.store_id
    AND CI.coffee_id = NEW.coffee_id;

    SELECT inventory_day INTO current_inventory_day
    FROM coffee_inventory CI
    WHERE CI.store_id = NEW.store_id
    AND CI.coffee_id = NEW.coffee_id;

    IF current_day != current_inventory_day THEN
        UPDATE carries SET inventory_day = current_day WHERE store_id = NEW.store_id AND coffee_id = NEW.coffee_id;
        UPDATE carries SET inventory = 200 - NEW.quantity WHERE store_id = NEW.store_id AND coffee_id = NEW.coffee_id;
        RAISE NOTICE 'The specific coffee was restocked for the day at the specified store.';

    ELSEIF NEW.quantity > current_inventory THEN
        RAISE EXCEPTION 'The store is currently out of stock for the specified coffee.';
    ELSE
        UPDATE carries SET inventory = inventory - NEW.quantity WHERE store_id = NEW.store_id AND coffee_id = NEW.coffee_id;
    END IF;

    RETURN NEW;
END;
$$
LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS check_inventory ON sale;
CREATE TRIGGER check_inventory
BEFORE INSERT ON sale FOR EACH ROW EXECUTE PROCEDURE check_inventory();

-- HW2 - Q10 View
CREATE VIEW customer_birthday AS
SELECT
    customer_id,
    month_of_birth,
    day_of_birth
FROM
    customer;

-- HW2 - Q10
-- when a coffee is sold on a customer’s birthday (i.e., a new sale is added to the
-- SALE table and the current date is the customer’s birthday), it updates the field 
-- quantity to reflect a BOGO (buy one get one free) offer. If the SALE quantity is 
-- greater than 1, i.e., the customer is purchasing at least two coffees of the same
-- type, the quantity of the SALE should be decreased by 1 to reflect the free beverage.
-- If the SALE quantity is not greater than 1, the trigger should not apply the BOGO
-- offer (i.e., the quantity will not be changed) and should raise the message 
-- ‘Two drinks must be purchased as part of this sale in order to apply your birthday
-- offer.’ Note that this offer can be applied multiple times for each SALE on the 
-- customer’s birthday.
CREATE OR REPLACE FUNCTION coffee_birthday_sale()
RETURNS trigger
AS
$$ DECLARE
    customer_month char(3);
    customer_day char(2);
    current_month char(3);
    current_day char(2);
BEGIN
    
SELECT month_of_birth INTO customer_month
FROM customer_birthday CB
WHERE NEW.customer_id = CB.customer_id;

SELECT day_of_birth INTO customer_day
FROM customer_birthday CB
WHERE NEW.customer_id = CB.customer_id;

SELECT LEFT(TO_CHAR(CURRENT_DATE, 'Month'), 3) INTO current_month;
SELECT EXTRACT(DAY FROM CURRENT_DATE) INTO current_day;

IF current_month = customer_month AND CAST(current_day AS INTEGER) = CAST(customer_day AS INTEGER) THEN
    IF NEW.quantity > 1 THEN
        NEW.quantity := CEILING(CAST(New.quantity AS FLOAT) / 2);
    ELSE
        RAISE NOTICE 'Two drinks must be purchased as part of this sale in order to apply BOGO birthday offer.';
    END IF;
ELSE
    RAISE NOTICE 'Customer does not have a birthday today.';
END IF;

RETURN NEW;

END;
$$
LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS coffee_birthday_sale ON sale;
CREATE TRIGGER coffee_birthday_sale
BEFORE INSERT ON sale FOR EACH ROW EXECUTE PROCEDURE coffee_birthday_sale();
