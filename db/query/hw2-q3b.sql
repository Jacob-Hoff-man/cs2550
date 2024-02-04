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
    promotion_count DESC