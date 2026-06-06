{{
    config(
        materialized     = 'incremental',
        unique_key       = 'review_id',
        on_schema_change = 'sync_all_columns'
    )
}}

WITH reviews AS (
    SELECT * FROM {{ ref('stg_reviews') }}

    {% if is_incremental() %}
        WHERE _extracted_at > (
            SELECT MAX(_extracted_at) FROM {{ this }}
        )
    {% endif %}
),

customers AS (
    SELECT customer_id, customer_key, full_name
    FROM {{ ref('dim_customers') }}
    WHERE is_current = TRUE
),

products AS (
    SELECT product_id, product_key, title, category
    FROM {{ ref('dim_products') }}
    WHERE is_current = TRUE
),

date_dim AS (
    SELECT date_key, full_date
    FROM {{ ref('dim_date') }}
)

SELECT
    r.review_id,
    r.order_id,
    r.product_id,
    p.product_key,
    p.title                                 AS product_title,
    p.category,
    r.customer_id,
    c.customer_key,
    c.full_name                             AS customer_name,
    d.date_key                              AS review_date_key,
    r.reviewed_at,
    r.rating,
    r.review_text,
    CASE
        WHEN r.rating >= 4.0 THEN 'positive'
        WHEN r.rating >= 3.0 THEN 'neutral'
        ELSE 'negative'
    END                                     AS sentiment,
    r._extracted_at
FROM reviews r
LEFT JOIN customers c
    ON r.customer_id = c.customer_id
LEFT JOIN products p
    ON r.product_id = p.product_id
LEFT JOIN date_dim d
    ON DATE_TRUNC('day', r.reviewed_at) = d.full_date