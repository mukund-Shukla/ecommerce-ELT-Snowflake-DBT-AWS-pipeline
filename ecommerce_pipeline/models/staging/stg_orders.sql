{{
    config(
        materialized = 'incremental',
        unique_key   = 'order_id',
        on_schema_change = 'sync_all_columns'
    )
}}

WITH source AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        status,
        payment_method,
        shipping_city,
        total_amount,
        updated_at,
        _extracted_at
    FROM {{ source('bronze', 'raw_orders') }}
    WHERE order_id IS NOT NULL
        AND customer_id IS NOT NULL
        AND total_amount > 0

    {% if is_incremental() %}
        AND _extracted_at > (
            SELECT MAX(_extracted_at) FROM {{ this }}
        )
    {% endif %}
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY _extracted_at DESC
        ) AS row_num
    FROM source
)

SELECT
    order_id,
    customer_id,
    order_date,
    LOWER(TRIM(status))                     AS status,
    LOWER(TRIM(payment_method))             AS payment_method,
    INITCAP(shipping_city)                  AS shipping_city,
    total_amount,
    updated_at,
    _extracted_at
FROM deduped
WHERE row_num = 1