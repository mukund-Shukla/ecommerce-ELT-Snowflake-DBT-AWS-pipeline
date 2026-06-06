{% snapshot snap_products %}

{{
    config(
        target_database = 'SNAPSHOTS_DB',
        target_schema   = 'SNAPSHOTS',
        unique_key      = 'product_id',
        strategy        = 'check',
        check_cols      = ['price', 'stock_quantity', 'cost_price'],
        invalidate_hard_deletes = True
    )
}}

SELECT
    product_id,
    title,
    category,
    price,
    cost_price,
    stock_quantity,
    gross_margin,
    rating,
    review_count,
    updated_at,
    _extracted_at
FROM {{ ref('stg_products') }}

{% endsnapshot %}