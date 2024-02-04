-- HW2 - Q3A
-- List the first name, last name, emails, and number of purchases of all customers who
-- have made more than one coffee purchase. List them in a descending order based
-- on the number of purchases.
SELECT
    fname,
    lname,
    email,
    COUNT(coffee_id) AS coffee_count
FROM
    CUSTOMER C
    NATURAL JOIN SALE S
WHERE
    C.customer_id = S.customer_id
GROUP BY
    customer_id
ORDER BY
    coffee_count DESC