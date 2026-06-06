SELECT
    product_id                              AS product_key,
    product_id,
    title,
    category,
    price,
    cost_price,
    gross_margin,
    stock_quantity,
    rating,
    review_count,
    updated_at,
    dbt_valid_from,
    dbt_valid_to,
    CASE
        WHEN dbt_valid_to IS NULL THEN TRUE
        ELSE FALSE
    END                                     AS is_current,
    dbt_scd_id
FROM {{ ref('snap_products') }}