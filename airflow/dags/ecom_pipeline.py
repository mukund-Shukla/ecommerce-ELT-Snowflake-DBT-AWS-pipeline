import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from alerting import on_failure_callback, on_success_callback, on_retry_callback

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data_engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'execution_timeout': timedelta(minutes=30),
    'depends_on_past': False,
    'email_on_failure': False,
    'on_failure_callback': on_failure_callback,
    'on_retry_callback': on_retry_callback
}

with DAG(
    dag_id='ecom_pipeline_daily',
    default_args=default_args,
    on_success_callback=on_success_callback,
    on_failure_callback=on_failure_callback,

    description='E-Commerce Order Analytics — daily medallion pipeline',
    schedule='0 6 * * *',
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    tags=['ecommerce', 'medallion', 'daily']
) as dag:

    def run_ingestion(**context):
        import sys
        sys.path.insert(0, '/opt/airflow/ingestion')
        from run_ingestion import run
        run()
        logger.info("Ingestion complete")

    t1_ingestion = PythonOperator(
        task_id='extract_to_s3',
        python_callable=run_ingestion
    )

    def copy_into_bronze(**context):
        import os
        import snowflake.connector

        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            role='LOADER_ROLE',
            warehouse='LOAD_WH',
            database='BRONZE_DB',
            schema='RAW'
        )

        copy_statements = [
            """
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
            ON_ERROR = 'CONTINUE'
            """,
            """
            COPY INTO raw_products (
                product_id, title, category, price, cost_price,
                stock_quantity, rating, review_count,
                updated_at, _extracted_at
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
            ON_ERROR = 'CONTINUE'
            """,
            """
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
            ON_ERROR = 'CONTINUE'
            """,
            """
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
            ON_ERROR = 'CONTINUE'
            """,
            """
            COPY INTO raw_reviews (
                review_id, order_id, product_id, customer_id,
                rating, review_text, reviewed_at,
                updated_at, _extracted_at
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
            ON_ERROR = 'CONTINUE'
            """
        ]

        try:
            cur = conn.cursor()
            for stmt in copy_statements:
                cur.execute(stmt)
                result = cur.fetchone()
                logger.info(f"COPY INTO result: {result}")
            logger.info("Bronze COPY INTO complete")
        except Exception as e:
            logger.error(f"COPY INTO failed: {e}")
            raise
        finally:
            conn.close()

    t2_copy_bronze = PythonOperator(
        task_id='copy_into_bronze',
        python_callable=copy_into_bronze
    )

    t3_dbt_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command='cd /opt/airflow/dbt && rm -rf target/ && dbt run --select staging --profiles-dir .'
    )

    t4_dbt_snapshots = BashOperator(
        task_id='dbt_snapshot',
        bash_command='cd /opt/airflow/dbt && dbt snapshot --profiles-dir .'
    )

    t5_dbt_test_silver = BashOperator(
        task_id='dbt_test_staging',
        bash_command='cd /opt/airflow/dbt && dbt test --select staging --profiles-dir .'
    )

    t5b_dbt_singular_tests = BashOperator(
    task_id='dbt_singular_tests',
    bash_command='cd /opt/airflow/dbt && dbt test --select test_type:singular --profiles-dir .'
    )

    t6_dbt_marts = BashOperator(
        task_id='dbt_run_marts',
        bash_command='cd /opt/airflow/dbt && dbt run --select marts --profiles-dir .'
    )

    t7_dbt_test_gold = BashOperator(
        task_id='dbt_test_marts',
        bash_command='cd /opt/airflow/dbt && dbt test --select marts --profiles-dir .'
    )

    t9_monitoring = BashOperator(
        task_id='dbt_run_monitoring',
        bash_command='cd /opt/airflow/dbt && dbt run --select monitoring_pipeline_health --profiles-dir .'
    )

    def validate_row_counts(**context):
        import os
        import snowflake.connector

        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            role='TRANSFORM_ROLE',
            warehouse='TRANSFORM_WH'
        )

        checks = [
            ('BRONZE_DB', 'RAW',     'raw_orders',           1),
            ('SILVER_DB', 'STAGING', 'stg_orders',           1),
            ('GOLD_DB',   'MARTS',   'fct_orders',           1),
            ('GOLD_DB',   'MARTS',   'mart_revenue_summary', 1),
        ]

        failed = []
        cur = conn.cursor()
        for db, schema, table, min_rows in checks:
            cur.execute(f'SELECT COUNT(*) FROM {db}.{schema}.{table}')
            count = cur.fetchone()[0]
            if count < min_rows:
                failed.append(f'{table} has {count} rows — expected >={min_rows}')
                logger.error(f'Row count check FAILED: {table} = {count}')
            else:
                logger.info(f'Row count check OK: {table} = {count}')

        conn.close()

        if failed:
            raise ValueError(f"Row count checks failed: {failed}")

        logger.info("All row count checks passed")

    t8_validate = PythonOperator(
        task_id='validate_row_counts',
        python_callable=validate_row_counts
    )

    (
        t1_ingestion
        >> t2_copy_bronze
        >> t3_dbt_staging
        >> t4_dbt_snapshots
        >> t5_dbt_test_silver
        >> t5b_dbt_singular_tests
        >> t6_dbt_marts
        >> t7_dbt_test_gold
        >> t8_validate
        >> t9_monitoring
    )