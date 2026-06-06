WITH date_spine AS (
    {{
        dbt_utils.date_spine(
            datepart   = "day",
            start_date = "cast('2024-01-01' as date)",
            end_date   = "cast('2027-12-31' as date)"
        )
    }}
)

SELECT
    DATE_PART('year', date_day) * 10000
    + DATE_PART('month', date_day) * 100
    + DATE_PART('day', date_day)            AS date_key,
    date_day                                AS full_date,
    DATE_PART('year', date_day)             AS year,
    DATE_PART('quarter', date_day)          AS quarter,
    DATE_PART('month', date_day)            AS month,
    MONTHNAME(date_day)                     AS month_name,
    DATE_PART('week', date_day)             AS week_of_year,
    DATE_PART('day', date_day)             AS day_of_month,
    DAYNAME(date_day)                       AS day_name,
    DATE_PART('dayofweek', date_day)        AS day_of_week,
    CASE
        WHEN DATE_PART('dayofweek', date_day) IN (0, 6)
        THEN TRUE ELSE FALSE
    END                                     AS is_weekend,
    CASE
        WHEN DATE_PART('month', date_day) IN (12, 1, 2) THEN 'Winter'
        WHEN DATE_PART('month', date_day) IN (3, 4, 5)  THEN 'Spring'
        WHEN DATE_PART('month', date_day) IN (6, 7, 8)  THEN 'Summer'
        ELSE 'Fall'
    END                                     AS season
FROM date_spine