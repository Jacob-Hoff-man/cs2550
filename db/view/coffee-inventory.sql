-- HW2 - Q9
CREATE VIEW coffee_inventory AS
SELECT
    store_id,
    coffee_id,
    inventory,
    inventory_day
FROM
    coffee CF NATURAL JOIN carries CA
WHERE
    CF.coffee_id = CA.coffee_id