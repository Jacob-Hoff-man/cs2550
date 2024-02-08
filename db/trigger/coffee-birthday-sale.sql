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