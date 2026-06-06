USE ROLE ACCOUNTADMIN;
USE DATABASE BRONZE_DB;
USE SCHEMA RAW;

-- file format for JSON
CREATE OR REPLACE FILE FORMAT ecom_json_format
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
    COMPRESSION = 'AUTO';

-- external stage pointing to S3
CREATE OR REPLACE STAGE ecom_raw_stage
    URL = 's3://ecom-order-pipeline-dev/'
    CREDENTIALS = (
        AWS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID'),
        AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY'),
    )
    FILE_FORMAT = ecom_json_format;

-- verify stage is working
LIST @ecom_raw_stage;