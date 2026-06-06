{{
    config(
        materialized = 'table'
    )
}}

SELECT
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.full_date                             AS order_date,
    p.category,
    COUNT(DISTINCT o.order_id)              AS total_orders,
    COUNT(DISTINCT o.customer_id)           AS unique_customers,
    SUM(o.total_amount)                     AS gross_revenue,
    AVG(o.total_amount)                     AS avg_order_value,
    COUNT(DISTINCT CASE
        WHEN o.status = 'delivered'
        THEN o.order_id END)                AS delivered_orders,
    COUNT(DISTINCT CASE
        WHEN o.status = 'cancelled'
        THEN o.order_id END)                AS cancelled_orders,
    ROUND(
        COUNT(DISTINCT CASE
            WHEN o.status = 'cancelled'
            THEN o.order_id END) * 100.0
        / NULLIF(COUNT(DISTINCT o.order_id), 0)
    , 2)                                    AS cancellation_rate_pct
FROM {{ ref('fct_orders') }} o
LEFT JOIN {{ ref('dim_date') }} d
    ON o.order_date_key = d.date_key
LEFT JOIN {{ ref('fct_order_items') }} oi
    ON o.order_id = oi.order_id
LEFT JOIN {{ ref('dim_products') }} p
    ON oi.product_id = p.product_id
    AND p.is_current = TRUE
GROUP BY 1, 2, 3, 4, 5, 6