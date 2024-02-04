-- HW2 - Q3E
-- List the top-3 customers with the highest number of coffee purchases. List cus-
-- tomer id, fname, lname, email, and number of coffee purchases.
SELECT
    customer_id,
    fname,
    lname,
    email,
    sum_coffee
FROM
    customer C
    NATURAL JOIN (
        SELECT
            customer_id,
            SUM(quantity) as sum_coffee
        FROM
            sale
        GROUP BY
            customer_id
    ) SQ
WHERE
    C.customer_id = SQ.customer_id
ORDER BY
    sum_coffee DESC
LIMIT
    3