WITH source AS (
    SELECT
        product_id,
        title,
        category,
        price,
        cost_price,
        stock_quantity,
        rating,
        review_count,
        updated_at,
        _extracted_at,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY _extracted_at DESC
        ) AS row_num
    FROM {{ source('bronze', 'raw_products') }}
    WHERE product_id IS NOT NULL
        AND price > 0
),

deduped AS (
    SELECT * FROM source WHERE row_num = 1
)

SELECT
    product_id,
    TRIM(title)                             AS title,
    LOWER(TRIM(category))                   AS category,
    price,
    cost_price,
    CASE
        WHEN stock_quantity < 0 THEN 0
        ELSE stock_quantity
    END                                     AS stock_quantity,
    rating,
    review_count,
    ROUND(price - cost_price, 2)            AS gross_margin,
    updated_at,
    _extracted_at
FROM deduped