FROM apache/airflow:3.2.2

USER root
RUN apt-get update && apt-get install -y git && apt-get clean

USER airflow

RUN pip install --no-cache-dir \
    boto3 \
    snowflake-connector-python \
    dbt-snowflake \
    python-dotenv

