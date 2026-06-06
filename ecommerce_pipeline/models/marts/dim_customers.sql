SELECT
    customer_id                             AS customer_key,
    customer_id,
    first_name,
    last_name,
    first_name || ' ' || last_name         AS full_name,
    email,
    username,
    phone,
    city,
    state,
    zipcode,
    country,
    created_at,
    updated_at,
    dbt_valid_from,
    dbt_valid_to,
    CASE
        WHEN dbt_valid_to IS NULL THEN TRUE
        ELSE FALSE
    END                                     AS is_current,
    dbt_scd_id
FROM {{ ref('snap_customers') }}