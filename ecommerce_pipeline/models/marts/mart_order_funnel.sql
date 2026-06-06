{{
    config(
        materialized = 'table'
    )
}}

WITH daily_funnel AS (
    SELECT
        DATE_TRUNC('day', order_date)       AS order_day,
        status,
        COUNT(DISTINCT order_id)            AS order_count,
        SUM(total_amount)                   AS revenue
    FROM {{ ref('fct_orders') }}
    GROUP BY 1, 2
)

SELECT
    order_day,
    SUM(order_count)                        AS total_orders,
    SUM(CASE WHEN status = 'pending'
        THEN order_count ELSE 0 END)        AS pending,
    SUM(CASE WHEN status = 'confirmed'
        THEN order_count ELSE 0 END)        AS confirmed,
    SUM(CASE WHEN status = 'shipped'
        THEN order_count ELSE 0 END)        AS shipped,
    SUM(CASE WHEN status = 'delivered'
        THEN order_count ELSE 0 END)        AS delivered,
    SUM(CASE WHEN status = 'cancelled'
        THEN order_count ELSE 0 END)        AS cancelled,
    SUM(CASE WHEN status = 'delivered'
        THEN revenue ELSE 0 END)            AS delivered_revenue,
    ROUND(
        SUM(CASE WHEN status = 'delivered'
            THEN order_count ELSE 0 END) * 100.0
        / NULLIF(SUM(order_count), 0)
    , 2)                                    AS delivery_rate_pct,
    ROUND(
        SUM(CASE WHEN status = 'cancelled'
            THEN order_count ELSE 0 END) * 100.0
        / NULLIF(SUM(order_count), 0)
    , 2)                                    AS cancellation_rate_pct
FROM daily_funnel
GROUP BY 1
ORDER BY 1