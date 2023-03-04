DROP FUNCTION IF EXISTS growth_of_average_check() CASCADE;

CREATE OR REPLACE FUNCTION growth_of_average_check(method int, date varchar,
        count_transactions bigint, coef_increase_avg_check double precision,
        max_charn_rate double precision, max_share_trans double precision, margin_share double precision)
RETURNS TABLE(customer_id bigint,
              Required_Check_Measure double precision,
              Group_Name varchar,
              Offer_Discount_Depth double precision) AS $$
    BEGIN
        IF (method = 1) THEN
            RETURN QUERY SELECT t2.customer_id,
                                (t1.Required_Check_Measure * coef_increase_avg_check) AS Required_Check_Measure,
                                sku_group.group_name, t2.Offer_Discount_Depth
                         FROM get_current_date(date) AS t1
                         JOIN determination_of_the_group(max_charn_rate, max_share_trans, margin_share) AS t2
                              ON t2.customer_id = t1.customer_id
                         JOIN sku_group ON sku_group.group_id = t2.Group_id;

        ELSEIF (method = 2) THEN
            RETURN QUERY SELECT t2.customer_id,
                                (t1.Required_Check_Measure * coef_increase_avg_check) AS Required_Check_Measure,
                                sku_group.group_name, t2.Offer_Discount_Depth
                         FROM get_average_check_n(count_transactions) AS t1
                         JOIN determination_of_the_group(max_charn_rate, max_share_trans, margin_share) AS t2
                                           ON t2.customer_id = t1.customer_id
                         JOIN sku_group ON sku_group.group_id = t2.Group_id;
        END IF;
    END;
$$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS get_current_date() CASCADE;

CREATE OR REPLACE FUNCTION get_current_date(cur_date varchar)
RETURNS TABLE(customer_id bigint, Required_Check_Measure double precision) AS $$
    DECLARE first_date date;
            last_date date;
    BEGIN
        first_date = split_part(cur_date, ' ', 1)::date;
        last_date = split_part(cur_date, ' ', 2)::date;
        RETURN QUERY SELECT * FROM get_average_check_date(first_date, last_date);
    END;
$$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS get_average_check_date() CASCADE;

CREATE OR REPLACE FUNCTION get_average_check_date(begin_date date, end_date date)
RETURNS TABLE(customer_id bigint, cur_trans_avg double precision) AS $$
    BEGIN
        CASE WHEN begin_date < first_transaction_date() THEN begin_date = first_transaction_date();
             WHEN end_date > last_transaction_date() THEN end_date = last_transaction_date();
             ELSE END CASE;

        RETURN QUERY SELECT cards.customer_card_id, AVG(transaction_summ)::double precision AS cur_trans_avg
                     FROM transactions
                     JOIN cards ON transactions.customer_card_id = cards.customer_card_id
                     WHERE transaction_datetime BETWEEN begin_date AND end_date
                     GROUP BY cards.customer_card_id;
    END;
$$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS first_transaction_date() CASCADE;

CREATE OR REPLACE FUNCTION first_transaction_date() RETURNS SETOF DATE AS $$
    BEGIN
        RETURN QUERY SELECT transaction_datetime::date
        FROM transactions
        ORDER BY transaction_datetime
        LIMIT 1;
    END;
$$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS last_transaction_date() CASCADE;

CREATE OR REPLACE FUNCTION last_transaction_date() RETURNS SETOF DATE AS $$
    BEGIN
        RETURN QUERY SELECT transaction_datetime::date
        FROM transactions
        ORDER BY transaction_datetime DESC
        LIMIT 1;
    END;
$$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS get_average_check_n() CASCADE;

CREATE OR REPLACE FUNCTION get_average_check_n(n bigint)
RETURNS TABLE (customer_id bigint, Required_Check_Measure double precision) AS $$
    BEGIN
        RETURN QUERY SELECT foo.customer_id, AVG(transaction_summ)
        FROM (SELECT cards.customer_id, transaction_summ, transaction_datetime,
                     ROW_NUMBER() OVER (PARTITION BY cards.customer_id ORDER BY transaction_datetime DESC) AS count
              FROM transactions
              JOIN cards ON transactions.customer_card_id = cards.customer_card_id
              ORDER BY cards.customer_card_id, transaction_datetime DESC) AS foo
        WHERE count <= n
        GROUP BY foo.customer_id;
    END;
$$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS sorted_group();

CREATE OR REPLACE FUNCTION sorted_group()
RETURNS TABLE(customer_id bigint, group_id bigint, group_affinity_index double precision,
              group_churn_rate double precision, group_discount_share double precision,
              group_minimum_discount double precision, av_margin double precision) AS $$
    BEGIN
        RETURN QUERY WITH cte_row_groups AS (SELECT *, RANK() OVER (PARTITION BY groups_view.customer_id ORDER BY groups_view.group_affinity_index DESC) AS number_id,
                                       AVG(group_margin) OVER (PARTITION BY groups_view.customer_id, groups_view.group_id) AS av_margin
                                FROM groups_view)
        SELECT cte_row_groups.customer_id, cte_row_groups.group_id, cte_row_groups.group_affinity_index,
               cte_row_groups.group_churn_rate, cte_row_groups.group_discount_share,
               cte_row_groups.group_minimum_discount, cte_row_groups.av_margin
        FROM cte_row_groups;
    END;
$$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS determination_of_the_group() CASCADE;

CREATE OR REPLACE FUNCTION determination_of_the_group(max_churn_index double precision,
        max_share_of_discount_transaction double precision, allowable_margin_share double precision)
RETURNS TABLE (customer_id bigint, Group_id bigint, Offer_Discount_Depth double precision) AS $$
    DECLARE id bigint := -1;
            value record;
            group_cur CURSOR FOR
                (SELECT *
                 FROM sorted_group());
            is_check bool := TRUE;
    BEGIN
        FOR value IN group_cur
            LOOP
                IF (is_check != TRUE AND id = value.customer_id) THEN
                    CONTINUE;
                END IF;
                IF (value.group_churn_rate <= max_churn_index AND
                   value.group_discount_share <= max_share_of_discount_transaction) THEN
                    IF (ABS(value.av_margin * allowable_margin_share / 100) >=
                        CEIL((value.group_minimum_discount * 100) / 5.0) * 0.05 * ABS(value.av_margin)) THEN
                        Customer_ID = value.customer_id;
                        Group_ID = value.group_id;
                        Offer_Discount_Depth = CEIL((value.group_minimum_discount * 100) / 5.0) * 5;
                        is_check = FALSE;
                        id = Customer_ID;
                        RETURN NEXT;
                    ELSE
                        is_check = TRUE;
                    END IF;
                ELSE
                    is_check = TRUE;
                END IF;
            END LOOP;
    END;
$$ LANGUAGE plpgsql;

SELECT *
from growth_of_average_check(2, '10.10.2020 10.10.2022', 200,  1.15, 3, 70, 30);
