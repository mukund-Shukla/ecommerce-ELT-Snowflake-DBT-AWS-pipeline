{{
    config(
        materialized     = 'incremental',
        unique_key       = 'order_id',
        on_schema_change = 'sync_all_columns'
    )
}}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}

    {% if is_incremental() %}
        WHERE _extracted_at > (
            SELECT MAX(_extracted_at) FROM {{ this }}
        )
    {% endif %}
),

-- join to current customer only (SCD2 aware)
customers AS (
    SELECT
        customer_id,
        customer_key,
        full_name,
        city,
        state
    FROM {{ ref('dim_customers') }}
    WHERE is_current = TRUE
),

-- join to current product snapshot not needed on fct_orders
-- product is on fct_order_items

date_dim AS (
    SELECT date_key, full_date
    FROM {{ ref('dim_date') }}
)

SELECT
    o.order_id,
    o.customer_id,
    c.customer_key,
    d.date_key                              AS order_date_key,
    o.order_date,
    o.status,
    o.payment_method,
    o.shipping_city,
    o.total_amount,
    o.updated_at,
    o._extracted_at
FROM orders o
LEFT JOIN customers c
    ON o.customer_id = c.customer_id
LEFT JOIN date_dim d
    ON DATE_TRUNC('day', o.order_date) = d.full_date