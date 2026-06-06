-- returns customer_id where two history rows overlap in time
-- zero rows = test passes

SELECT
    a.customer_id,
    a.dbt_valid_from    AS a_valid_from,
    a.dbt_valid_to      AS a_valid_to,
    b.dbt_valid_from    AS b_valid_from,
    b.dbt_valid_to      AS b_valid_to
FROM {{ ref('dim_customers') }} a
JOIN {{ ref('dim_customers') }} b
    ON a.customer_id = b.customer_id
    AND a.dbt_scd_id != b.dbt_scd_id
    AND a.dbt_valid_from < COALESCE(b.dbt_valid_to, '9999-12-31')
    AND COALESCE(a.dbt_valid_to, '9999-12-31') > b.dbt_valid_from
    AND a.dbt_valid_from < b.dbt_valid_from