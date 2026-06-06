USE ROLE SYSADMIN;
USE DATABASE BRONZE_DB;
USE SCHEMA RAW;
USE WAREHOUSE LOAD_WH;

-- ── CUSTOMERS ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_customers (
    customer_id         NUMBER,
    first_name          VARCHAR,
    last_name           VARCHAR,
    email               VARCHAR,
    username            VARCHAR,
    phone               VARCHAR,
    city                VARCHAR,
    state               VARCHAR,
    zipcode             VARCHAR,
    country             VARCHAR,
    created_at          TIMESTAMP_NTZ,
    updated_at          TIMESTAMP_NTZ,
    _extracted_at       TIMESTAMP_NTZ,
    _load_id            VARCHAR DEFAULT md5(TO_VARCHAR(CURRENT_TIMESTAMP()))
);

-- ── PRODUCTS ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_products (
    product_id          NUMBER,
    title               VARCHAR,
    category            VARCHAR,
    price               FLOAT,
    cost_price          FLOAT,
    stock_quantity      NUMBER,
    rating              FLOAT,
    review_count        NUMBER,
    updated_at          TIMESTAMP_NTZ,
    _extracted_at       TIMESTAMP_NTZ,
    _load_id            VARCHAR DEFAULT md5(TO_VARCHAR(CURRENT_TIMESTAMP()))
);

-- ── ORDERS ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_orders (
    order_id            NUMBER,
    customer_id         NUMBER,
    order_date          TIMESTAMP_NTZ,
    status              VARCHAR,
    payment_method      VARCHAR,
    shipping_city       VARCHAR,
    total_amount        FLOAT,
    updated_at          TIMESTAMP_NTZ,
    _extracted_at       TIMESTAMP_NTZ,
    _load_id            VARCHAR DEFAULT md5(TO_VARCHAR(CURRENT_TIMESTAMP()))
);

-- ── ORDER ITEMS ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_order_items (
    order_item_id       NUMBER,
    order_id            NUMBER,
    product_id          NUMBER,
    quantity            NUMBER,
    unit_price          FLOAT,
    line_total          FLOAT,
    _extracted_at       TIMESTAMP_NTZ,
    _load_id            VARCHAR DEFAULT md5(TO_VARCHAR(CURRENT_TIMESTAMP()))
);

-- ── REVIEWS ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_reviews (
    review_id           NUMBER,
    order_id            NUMBER,
    product_id          NUMBER,
    customer_id         NUMBER,
    rating              FLOAT,
    review_text         VARCHAR,
    reviewed_at         TIMESTAMP_NTZ,
    updated_at          TIMESTAMP_NTZ,
    _extracted_at       TIMESTAMP_NTZ,
    _load_id            VARCHAR DEFAULT md5(TO_VARCHAR(CURRENT_TIMESTAMP()))
);