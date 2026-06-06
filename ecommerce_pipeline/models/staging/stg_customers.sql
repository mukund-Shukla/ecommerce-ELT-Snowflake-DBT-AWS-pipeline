WITH source AS (
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
        _extracted_at,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY _extracted_at DESC
        ) AS row_num
    FROM {{ source('bronze', 'raw_customers') }}
    WHERE customer_id IS NOT NULL
),

deduped AS (
    SELECT * FROM source WHERE row_num = 1
)

SELECT
    customer_id,
    TRIM(first_name)                        AS first_name,
    TRIM(last_name)                         AS last_name,
    LOWER(TRIM(email))                      AS email,
    LOWER(TRIM(username))                   AS username,
    phone,
    INITCAP(city)                           AS city,
    UPPER(state)                            AS state,
    zipcode,
    UPPER(country)                          AS country,
    created_at,
    updated_at,
    _extracted_at
FROM deduped