-- HW2 - Q10
CREATE VIEW customer_birthday AS
SELECT
    customer_id,
    month_of_birth,
    day_of_birth
FROM
    customer