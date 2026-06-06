-- returns orders with zero or negative revenue
-- zero rows = test passes

SELECT
    order_id,
    total_amount
FROM {{ ref('fct_orders') }}
WHERE total_amount <= 0