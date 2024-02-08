-- Create a view named LAST QUARTER PERFORMACE that lists the num-
-- ber of customers that each store sold coffee to during the fourth quarter of 2023 (i.e.,
-- September 1st, 2023 to December 31, 2023). The view schema is: store id, store name,
-- store type, and number of customers.
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
    type