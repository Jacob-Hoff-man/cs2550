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