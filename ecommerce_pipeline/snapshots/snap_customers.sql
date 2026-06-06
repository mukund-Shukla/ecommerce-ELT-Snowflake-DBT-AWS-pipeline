{% snapshot snap_customers %}

{{
    config(
        target_database = 'SNAPSHOTS_DB',
        target_schema   = 'SNAPSHOTS',
        unique_key      = 'customer_id',
        strategy        = 'check',
        check_cols      = ['city', 'state', 'phone', 'email'],
        invalidate_hard_deletes = True
    )
}}

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    username,
    phone,
    city,
    state,
    zipcode,
    country,
    created_at,
    updated_at,
    _extracted_at
FROM {{ ref('stg_customers') }}

{% endsnapshot %}