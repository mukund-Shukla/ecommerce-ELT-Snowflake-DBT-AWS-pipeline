{{
    config(
        materialized = 'incremental',
        unique_key   = 'order_item_id',
        on_schema_change = 'sync_all_columns'
    )
}}

WITH source AS (
    SELECT
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        line_total,
        _extracted_at
    FROM {{ source('bronze', 'raw_order_items') }}
    WHERE order_item_id IS NOT NULL
        AND order_id IS NOT NULL
        AND quantity > 0
        AND unit_price > 0

    {% if is_incremental() %}
        AND _extracted_at > (
            SELECT MAX(_extracted_at) FROM {{ this }}
        )
    {% endif %}
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_item_id
            ORDER BY _extracted_at DESC
        ) AS row_num
    FROM source
)

SELECT
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    line_total,
    ROUND(unit_price * quantity, 2)         AS calculated_line_total,
    _extracted_at
FROM deduped
WHERE row_num = 1