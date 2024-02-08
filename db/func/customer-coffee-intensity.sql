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