USE ROLE LOADER_ROLE;
USE DATABASE BRONZE_DB;
USE SCHEMA RAW;
USE WAREHOUSE LOAD_WH;

-- ── CUSTOMERS ──────────────────────────────────────────────────────
COPY INTO raw_customers (
    customer_id, first_name, last_name, email, username,
    phone, city, state, zipcode, country,
    created_at, updated_at, _extracted_at
)
FROM (
    SELECT
        $1:customer_id::NUMBER,
        $1:first_name::VARCHAR,
        $1:last_name::VARCHAR,
        $1:email::VARCHAR,
        $1:username::VARCHAR,
        $1:phone::VARCHAR,
        $1:city::VARCHAR,
        $1:state::VARCHAR,
        $1:zipcode::VARCHAR,
        $1:country::VARCHAR,
        $1:created_at::TIMESTAMP_NTZ,
        $1:updated_at::TIMESTAMP_NTZ,
        $1:_extracted_at::TIMESTAMP_NTZ
    FROM @ecom_raw_stage/customers/snapshot/
)
FILE_FORMAT = (FORMAT_NAME = 'ecom_json_format')
ON_ERROR = 'CONTINUE';

-- ── PRODUCTS ───────────────────────────────────────────────────────
COPY INTO raw_products (
    product_id, title, category, price, cost_price,
    stock_quantity, rating, review_count, updated_at, _extracted_at
)
FROM (
    SELECT
        $1:product_id::NUMBER,
        $1:title::VARCHAR,
        $1:category::VARCHAR,
        $1:price::FLOAT,
        $1:cost_price::FLOAT,
        $1:stock_quantity::NUMBER,
        $1:rating::FLOAT,
        $1:review_count::NUMBER,
        $1:updated_at::TIMESTAMP_NTZ,
        $1:_extracted_at::TIMESTAMP_NTZ
    FROM @ecom_raw_stage/products/snapshot/
)
FILE_FORMAT = (FORMAT_NAME = 'ecom_json_format')
ON_ERROR = 'CONTINUE';

-- ── ORDERS ─────────────────────────────────────────────────────────
COPY INTO raw_orders (
    order_id, customer_id, order_date, status,
    payment_method, shipping_city, total_amount,
    updated_at, _extracted_at
)
FROM (
    SELECT
        $1:order_id::NUMBER,
        $1:customer_id::NUMBER,
        $1:order_date::TIMESTAMP_NTZ,
        $1:status::VARCHAR,
        $1:payment_method::VARCHAR,
        $1:shipping_city::VARCHAR,
        $1:total_amount::FLOAT,
        $1:updated_at::TIMESTAMP_NTZ,
        $1:_extracted_at::TIMESTAMP_NTZ
    FROM @ecom_raw_stage/orders/incremental/
)
FILE_FORMAT = (FORMAT_NAME = 'ecom_json_format')
ON_ERROR = 'CONTINUE';

-- ── ORDER ITEMS ────────────────────────────────────────────────────
COPY INTO raw_order_items (
    order_item_id, order_id, product_id,
    quantity, unit_price, line_total, _extracted_at
)
FROM (
    SELECT
        $1:order_item_id::NUMBER,
        $1:order_id::NUMBER,
        $1:product_id::NUMBER,
        $1:quantity::NUMBER,
        $1:unit_price::FLOAT,
        $1:line_total::FLOAT,
        $1:_extracted_at::TIMESTAMP_NTZ
    FROM @ecom_raw_stage/order_items/incremental/
)
FILE_FORMAT = (FORMAT_NAME = 'ecom_json_format')
ON_ERROR = 'CONTINUE';

-- ── REVIEWS ────────────────────────────────────────────────────────
COPY INTO raw_reviews (
    review_id, order_id, product_id, customer_id,
    rating, review_text, reviewed_at, updated_at, _extracted_at
)
FROM (
    SELECT
        $1:review_id::NUMBER,
        $1:order_id::NUMBER,
        $1:product_id::NUMBER,
        $1:customer_id::NUMBER,
        $1:rating::FLOAT,
        $1:review_text::VARCHAR,
        $1:reviewed_at::TIMESTAMP_NTZ,
        $1:updated_at::TIMESTAMP_NTZ,
        $1:_extracted_at::TIMESTAMP_NTZ
    FROM @ecom_raw_stage/reviews/incremental/
)
FILE_FORMAT = (FORMAT_NAME = 'ecom_json_format')
ON_ERROR = 'CONTINUE';