{{
    config(
        materialized = 'table'
    )
}}

SELECT
    p.product_id,
    p.title                                 AS product_name,
    p.category,
    p.price                                 AS current_price,
    p.cost_price,
    p.gross_margin,
    COUNT(DISTINCT oi.order_id)             AS total_orders,
    SUM(oi.quantity)                        AS total_units_sold,
    SUM(oi.line_total)                      AS total_revenue,
    AVG(oi.unit_price)                      AS avg_selling_price,
    ROUND(
        SUM(oi.line_total) - SUM(oi.quantity * p.cost_price)
    , 2)                                    AS total_profit,
    COUNT(DISTINCT r.review_id)             AS total_reviews,
    ROUND(AVG(r.rating), 2)                 AS avg_rating,
    COUNT(DISTINCT CASE
        WHEN r.sentiment = 'positive'
        THEN r.review_id END)               AS positive_reviews,
    COUNT(DISTINCT CASE
        WHEN r.sentiment = 'negative'
        THEN r.review_id END)               AS negative_reviews
FROM {{ ref('dim_products') }} p
LEFT JOIN {{ ref('fct_order_items') }} oi
    ON p.product_id = oi.product_id
LEFT JOIN {{ ref('fct_reviews') }} r
    ON p.product_id = r.product_id
WHERE p.is_current = TRUE
GROUP BY 1, 2, 3, 4, 5, 6