-- returns reviews with rating outside 1.0 to 5.0
-- zero rows = test passes

SELECT
    review_id,
    rating
FROM {{ ref('fct_reviews') }}
WHERE rating < 1.0
   OR rating > 5.0