{{
    config(materialized = 'table')
}}

WITH bronze_counts AS (
    SELECT
        'raw_customers'                     AS table_name,
        'bronze'                            AS layer,
        COUNT(*)                            AS row_count,
        MAX(_extracted_at)                  AS last_loaded_at,
        DATEDIFF('hour',
            MAX(_extracted_at),
            CURRENT_TIMESTAMP())            AS hours_since_load
    FROM {{ source('bronze', 'raw_customers') }}

    UNION ALL

    SELECT
        'raw_products', 'bronze',
        COUNT(*), MAX(_extracted_at),
        DATEDIFF('hour', MAX(_extracted_at), CURRENT_TIMESTAMP())
    FROM {{ source('bronze', 'raw_products') }}

    UNION ALL

    SELECT
        'raw_orders', 'bronze',
        COUNT(*), MAX(_extracted_at),
        DATEDIFF('hour', MAX(_extracted_at), CURRENT_TIMESTAMP())
    FROM {{ source('bronze', 'raw_orders') }}

    UNION ALL

    SELECT
        'raw_order_items', 'bronze',
        COUNT(*), MAX(_extracted_at),
        DATEDIFF('hour', MAX(_extracted_at), CURRENT_TIMESTAMP())
    FROM {{ source('bronze', 'raw_order_items') }}

    UNION ALL

    SELECT
        'raw_reviews', 'bronze',
        COUNT(*), MAX(_extracted_at),
        DATEDIFF('hour', MAX(_extracted_at), CURRENT_TIMESTAMP())
    FROM {{ source('bronze', 'raw_reviews') }}
),

silver_counts AS (
    SELECT
        'stg_orders'                        AS table_name,
        'silver'                            AS layer,
        COUNT(*)                            AS row_count,
        MAX(_extracted_at)                  AS last_loaded_at,
        DATEDIFF('hour',
            MAX(_extracted_at),
            CURRENT_TIMESTAMP())            AS hours_since_load
    FROM {{ ref('stg_orders') }}

    UNION ALL

    SELECT
        'stg_order_items', 'silver',
        COUNT(*), MAX(_extracted_at),
        DATEDIFF('hour', MAX(_extracted_at), CURRENT_TIMESTAMP())
    FROM {{ ref('stg_order_items') }}

    UNION ALL

    SELECT
        'stg_reviews', 'silver',
        COUNT(*), MAX(_extracted_at),
        DATEDIFF('hour', MAX(_extracted_at), CURRENT_TIMESTAMP())
    FROM {{ ref('stg_reviews') }}
),

gold_counts AS (
    SELECT
        'fct_orders'                        AS table_name,
        'gold'                              AS layer,
        COUNT(*)                            AS row_count,
        MAX(_extracted_at)                  AS last_loaded_at,
        DATEDIFF('hour',
            MAX(_extracted_at),
            CURRENT_TIMESTAMP())            AS hours_since_load
    FROM {{ ref('fct_orders') }}

    UNION ALL

    SELECT
        'fct_order_items', 'gold',
        COUNT(*), MAX(_extracted_at),
        DATEDIFF('hour', MAX(_extracted_at), CURRENT_TIMESTAMP())
    FROM {{ ref('fct_order_items') }}

    UNION ALL

    SELECT
        'fct_reviews', 'gold',
        COUNT(*), MAX(_extracted_at),
        DATEDIFF('hour', MAX(_extracted_at), CURRENT_TIMESTAMP())
    FROM {{ ref('fct_reviews') }}
),

all_counts AS (

    SELECT * FROM bronze_counts

    UNION ALL

    SELECT * FROM silver_counts

    UNION ALL

    SELECT * FROM gold_counts

)

SELECT
    table_name,
    layer,
    row_count,
    last_loaded_at,
    hours_since_load,
    CASE
        WHEN hours_since_load <= 25 THEN 'healthy'
        WHEN hours_since_load <= 48 THEN 'warning'
        ELSE 'critical'
    END AS freshness_status
FROM all_counts
ORDER BY layer, table_name
