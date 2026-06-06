-- 1 — How is daily revenue trending over the last month?

SELECT
    order_date,
    SUM(gross_revenue)          AS total_revenue,
    SUM(total_orders)           AS total_orders,
    ROUND(SUM(gross_revenue) 
        / NULLIF(SUM(total_orders), 0), 2) AS avg_order_value
FROM GOLD_DB.MARTS.mart_revenue_summary
WHERE order_date >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY order_date
ORDER BY order_date DESC;



-- 2 — Which products are driving the most revenue and are they profitable?

SELECT
    product_name,
    category,
    total_units_sold,
    total_revenue,
    total_profit,
    avg_rating
FROM GOLD_DB.MARTS.mart_product_performance
ORDER BY total_revenue DESC
LIMIT 10;


-- 3 — How are customers distributed across segments and what is each segment worth?


SELECT
    customer_segment,
    COUNT(*)                    AS total_customers,
    ROUND(AVG(lifetime_value), 2) AS avg_lifetime_value,
    ROUND(AVG(total_orders), 2) AS avg_orders,
    ROUND(AVG(recency_days), 2) AS avg_days_since_last_order
FROM GOLD_DB.MARTS.mart_customer_orders
GROUP BY customer_segment
ORDER BY avg_lifetime_value DESC;


-- 4 - Which products had price changes and what was the full price history?

SELECT
    product_id,
    title,
    category,
    price                       AS historical_price,
    dbt_valid_from              AS price_effective_from,
    dbt_valid_to                AS price_effective_to,
    CASE
        WHEN dbt_valid_to IS NULL THEN 'current'
        ELSE 'historical'
    END                         AS record_status
FROM GOLD_DB.MARTS.dim_products
WHERE product_id IN (
    SELECT product_id
    FROM GOLD_DB.MARTS.dim_products
    GROUP BY product_id
    HAVING COUNT(*) > 1
)
ORDER BY product_id, dbt_valid_from;

-- 5 -  Which categories are growing month over month and by how much?
SELECT
    year,
    month,
    month_name,
    category,
    SUM(gross_revenue)          AS monthly_revenue,
    SUM(total_orders)           AS monthly_orders,
    ROUND(
        (SUM(gross_revenue) - LAG(SUM(gross_revenue))
            OVER (PARTITION BY category ORDER BY year, month))
        / NULLIF(LAG(SUM(gross_revenue))
            OVER (PARTITION BY category ORDER BY year, month), 0) * 100
    , 2)                        AS revenue_growth_pct
FROM GOLD_DB.MARTS.mart_revenue_summary
GROUP BY year, month, month_name, category
ORDER BY year DESC, month DESC, monthly_revenue DESC;



-- 6- What was the price of the ordered item at the time of order vs current price?
SELECT
    oi.order_item_id,
    oi.order_id,
    oi.product_id,
    oi.unit_price                           AS price_at_order_time,  -- actual price paid
    p_current.price                         AS price_today,
    ROUND(p_current.price - oi.unit_price, 2) AS price_difference
FROM fct_order_items oi
LEFT JOIN dim_products p_current
    ON oi.product_id = p_current.product_id
    AND p_current.is_current = TRUE
LIMIT 20;

-- 7- Which customer segment contributes the highest revenue?

SELECT
    customer_segment,
    COUNT(*) AS customers,
    ROUND(SUM(lifetime_value), 2) AS total_revenue,
    ROUND(AVG(lifetime_value), 2) AS avg_ltv
FROM GOLD_DB.MARTS.mart_customer_orders
GROUP BY customer_segment
ORDER BY total_revenue DESC;


-- 8 - Which customers are at risk of churning?

SELECT
    customer_id,
    customer_segment,
    lifetime_value,
    total_orders,
    recency_days
FROM GOLD_DB.MARTS.mart_customer_orders
WHERE customer_segment = 'At Risk'
ORDER BY lifetime_value DESC;


-- 9 Which categories have the highest average order value (AOV)?
SELECT
    category,
    ROUND(
        SUM(gross_revenue) / NULLIF(SUM(total_orders),0),
        2
    ) AS avg_order_value,
    SUM(gross_revenue) AS revenue
FROM GOLD_DB.MARTS.mart_revenue_summary
GROUP BY category
ORDER BY avg_order_value DESC;


--10 Best selling product in each category

SELECT *
FROM (
    SELECT
        category,
        product_name,
        total_units_sold,
        ROW_NUMBER() OVER(
            PARTITION BY category
            ORDER BY total_units_sold DESC
        ) rn
    FROM GOLD_DB.MARTS.mart_product_performance
)
WHERE rn = 1;

-- 11 Daily GMV by Category

SELECT
    order_date,
    category,
    SUM(gross_revenue) AS daily_gmv
FROM GOLD_DB.MARTS.mart_revenue_summary
GROUP BY
    order_date,
    category
ORDER BY
    order_date DESC,
    daily_gmv DESC;