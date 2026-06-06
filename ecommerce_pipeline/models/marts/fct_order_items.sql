{{
    config(
        materialized     = 'incremental',
        unique_key       = 'order_item_id',
        on_schema_change = 'sync_all_columns'
    )
}}

WITH order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}

    {% if is_incremental() %}
        WHERE _extracted_at > (
            SELECT MAX(_extracted_at) FROM {{ this }}
        )
    {% endif %}
),

orders AS (
    SELECT order_id, order_date
    FROM {{ ref('stg_orders') }}
),

products AS (
    SELECT
        product_id,
        product_key,
        title,
        category,
        price AS current_price,
        dbt_valid_from,
        dbt_valid_to,
        is_current
    FROM {{ ref('dim_products') }}
    WHERE is_current = TRUE
)

SELECT
    oi.order_item_id,
    oi.order_id,
    oi.product_id,
    p.product_key,
    p.title                                 AS product_title,
    p.category,
    oi.quantity,
    oi.unit_price,
    oi.line_total,
    oi.calculated_line_total,
    p.current_price,
    oi._extracted_at
FROM order_items oi
LEFT JOIN orders o
    ON oi.order_id = o.order_id
LEFT JOIN products p
    ON oi.product_id = p.product_id