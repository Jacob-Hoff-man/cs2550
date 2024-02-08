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