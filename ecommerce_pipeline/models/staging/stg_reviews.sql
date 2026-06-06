{{
    config(
        materialized = 'incremental',
        unique_key   = 'review_id',
        on_schema_change = 'sync_all_columns'
    )
}}

WITH source AS (
    SELECT
        review_id,
        order_id,
        product_id,
        customer_id,
        rating,
        review_text,
        reviewed_at,
        updated_at,
        _extracted_at
    FROM {{ source('bronze', 'raw_reviews') }}
    WHERE review_id IS NOT NULL
        AND rating BETWEEN 1.0 AND 5.0

    {% if is_incremental() %}
        AND _extracted_at > (
            SELECT MAX(_extracted_at) FROM {{ this }}
        )
    {% endif %}
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY review_id
            ORDER BY _extracted_at DESC
        ) AS row_num
    FROM source
)

SELECT
    review_id,
    order_id,
    product_id,
    customer_id,
    rating,
    TRIM(review_text)                       AS review_text,
    reviewed_at,
    updated_at,
    _extracted_at
FROM deduped
WHERE row_num = 1