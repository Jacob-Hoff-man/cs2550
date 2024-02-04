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
    store_id ASC