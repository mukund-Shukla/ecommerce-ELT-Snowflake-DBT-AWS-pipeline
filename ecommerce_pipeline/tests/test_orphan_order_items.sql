-- returns order_items with no matching order
-- zero rows = test passes

SELECT
    oi.order_item_id,
    oi.order_id
FROM {{ ref('fct_order_items') }} oi
LEFT JOIN {{ ref('fct_orders') }} o
    ON oi.order_id = o.order_id
WHERE o.order_id IS NULL