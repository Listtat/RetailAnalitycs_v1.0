SET datestyle TO ISO,DMY;

DROP FUNCTION IF EXISTS receiving_remuneration CASCADE;

CREATE OR REPLACE FUNCTION receiving_remuneration(Date_beginning timestamp, Date_completion timestamp,
        count_transactions bigint, max_churn_index double precision,
        max_share_of_discount_transaction double precision, allowable_margin_share double precision)
RETURNS TABLE (customer_id bigint, Start_Date timestamp, End_Date timestamp,
                Required_Transactions_Count double precision, Group_Name varchar,
                Offer_Discount_Depth double precision) AS $$
    BEGIN
        RETURN QUERY SELECT t1.customer_id, Date_beginning, Date_completion,
                            t1.Required_Transactions_Count AS Required_Transactions_Count,
                            sku_group.group_name AS Group_Name, t2.offer_discount_depth AS Offer_Discount_Depth
                     FROM defining_an_offer_condition(Date_beginning, Date_completion, count_transactions) AS t1
                     JOIN determination_of_the_group(max_churn_index, max_share_of_discount_transaction,
                         allowable_margin_share) AS t2 ON t1.customer_id = t2.customer_id
                     JOIN sku_group ON sku_group.group_id = t2.group_id;
        END;
$$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS defining_an_offer_condition CASCADE;

CREATE OR REPLACE FUNCTION defining_an_offer_condition(Date_beginning timestamp, Date_completion timestamp,
            count_transactions bigint)
RETURNS TABLE (customer_id bigint, Start_Date timestamp, End_Date timestamp,
                Required_Transactions_Count double precision) AS $$
    BEGIN
        RETURN QUERY SELECT customers_view.customer_id, Date_beginning AS Start_Date, Date_completion AS End_Date,
            (ROUND((Date_completion::date - Date_beginning::date) / customers_view.Customer_Frequency) +
                                    count_transactions)::double precision AS Required_Transactions_Count
                     FROM customers_view;
    END;
$$ LANGUAGE plpgsql;

SELECT *
FROM receiving_remuneration('18.08.2020', '18.08.2022', 1,3,70,30);