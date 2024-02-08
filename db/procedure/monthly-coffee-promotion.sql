-- HW2 - Q8
-- creates a promotion with the start date set to the day of the procedure call 
-- with the end date set to 30 days after the day of the procedure call. 
-- In addition, the procedure should promote a specific coffee. 
-- The procedure has the following input: coffee_id, promotion_id, promotion_name
CREATE
OR REPLACE PROCEDURE monthly_coffee_promotion_proc(
    inp_coffee_id integer,
    inp_promotion_id integer,
    inp_promotion_name varchar(60)
) LANGUAGE plpgsql AS $$ BEGIN

INSERT INTO
    promotion
    (
        promotion_id,
        name,
        start_date,
        end_date
    )
VALUES
    (
        inp_promotion_id,
        inp_promotion_name,
        CURRENT_DATE,
        CURRENT_DATE + 30
    );

INSERT INTO
    promotes
    (
        promotion_id,
        coffee_id
    )
VALUES
    (
        inp_promotion_id,
        inp_coffee_id
    );

END;

$$;