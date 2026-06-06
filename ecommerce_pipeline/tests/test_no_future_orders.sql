-- returns orders with date in the future
-- zero rows = test passes

SELECT
    order_id,
    order_date
FROM {{ ref('fct_orders') }}
WHERE DATE_TRUNC('day', order_date) > CURRENT_DATE()