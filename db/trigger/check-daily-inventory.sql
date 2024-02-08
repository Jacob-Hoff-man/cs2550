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