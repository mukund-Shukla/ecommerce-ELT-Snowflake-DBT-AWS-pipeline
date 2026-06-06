-- returns customer_ids with more than one current record
-- zero rows = test passes

SELECT
    customer_id,
    COUNT(*) AS current_row_count
FROM {{ ref('dim_customers') }}
WHERE is_current = TRUE
GROUP BY customer_id
HAVING COUNT(*) > 1