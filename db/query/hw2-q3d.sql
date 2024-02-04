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
    C.coffee_id = TQ.coffee_id