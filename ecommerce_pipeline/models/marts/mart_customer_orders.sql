{{
    config(
        materialized = 'table'
    )
}}

WITH order_stats AS (
    SELECT
        customer_id,
        COUNT(DISTINCT order_id)            AS total_orders,
        SUM(total_amount)                   AS lifetime_value,
        AVG(total_amount)                   AS avg_order_value,
        MIN(order_date)                     AS first_order_date,
        MAX(order_date)                     AS last_order_date,
        DATEDIFF('day',
            MIN(order_date),
            MAX(order_date))                AS customer_tenure_days,
        COUNT(DISTINCT CASE
            WHEN status = 'delivered'
            THEN order_id END)              AS delivered_orders,
        COUNT(DISTINCT CASE
            WHEN status = 'cancelled'
            THEN order_id END)              AS cancelled_orders
    FROM {{ ref('fct_orders') }}
    GROUP BY 1
),

-- RFM scoring
rfm AS (
    SELECT
        customer_id,
        DATEDIFF('day', MAX(order_date), CURRENT_TIMESTAMP()) AS recency_days,
        COUNT(DISTINCT order_id)            AS frequency,
        SUM(total_amount)                   AS monetary
    FROM {{ ref('fct_orders') }}
    WHERE status != 'cancelled'
    GROUP BY 1
),

rfm_scored AS (
    SELECT
        customer_id,
        recency_days,
        frequency,
        monetary,
        NTILE(5) OVER (ORDER BY recency_days ASC)   AS recency_score,
        NTILE(5) OVER (ORDER BY frequency DESC)      AS frequency_score,
        NTILE(5) OVER (ORDER BY monetary DESC)       AS monetary_score
    FROM rfm
)

SELECT
    c.customer_id,
    c.full_name,
    c.email,
    c.city,
    c.state,
    c.country,
    os.total_orders,
    os.lifetime_value,
    os.avg_order_value,
    os.first_order_date,
    os.last_order_date,
    os.customer_tenure_days,
    os.delivered_orders,
    os.cancelled_orders,
    rs.recency_days,
    rs.frequency,
    rs.monetary,
    rs.recency_score,
    rs.frequency_score,
    rs.monetary_score,
    ROUND(
        (rs.recency_score + rs.frequency_score + rs.monetary_score) / 3.0
    , 2)                                    AS rfm_score,
    CASE
        WHEN rs.recency_score >= 4
            AND rs.frequency_score >= 4     THEN 'Champion'
        WHEN rs.recency_score >= 3
            AND rs.frequency_score >= 3     THEN 'Loyal'
        WHEN rs.recency_score >= 4
            AND rs.frequency_score < 3      THEN 'Recent'
        WHEN rs.recency_score < 3
            AND rs.frequency_score >= 3     THEN 'At Risk'
        ELSE 'Needs Attention'
    END                                     AS customer_segment
FROM {{ ref('dim_customers') }} c
LEFT JOIN order_stats os
    ON c.customer_id = os.customer_id
LEFT JOIN rfm_scored rs
    ON c.customer_id = rs.customer_id
WHERE c.is_current = TRUE