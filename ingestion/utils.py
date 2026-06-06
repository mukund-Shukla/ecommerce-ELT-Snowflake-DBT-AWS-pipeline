import boto3
import json
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def get_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION')
    )


def _build_key(entity: str, dt: datetime) -> str:
    mode = 'snapshot' if entity in ('customers', 'products') else 'incremental'
    return (
        f"{entity}/{mode}/"
        f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
        f"{entity}.json"
    )


def upload_to_s3(records: list, entity: str, dt: datetime = None) -> str:
    if not records:
        logger.warning(f"No records for {entity} — skipping upload")
        return None

    if dt is None:
        dt = datetime.utcnow()

    bucket = os.getenv('S3_BUCKET')
    if not bucket:
        raise ValueError("S3_BUCKET environment variable not set")

    key = _build_key(entity, dt)
    ndjson = '\n'.join(json.dumps(r) for r in records)

    try:
        get_s3_client().put_object(
            Bucket=bucket,
            Key=key,
            Body=ndjson.encode('utf-8'),
            ContentType='application/x-ndjson'
        )
        logger.info(f"Uploaded {len(records)} records → s3://{bucket}/{key}")
        return key
    except Exception as e:
        logger.error(f"Upload failed for {entity}: {e}")
        raise


def load_from_s3(entity: str, dt: datetime = None) -> list:
    if dt is None:
        dt = datetime.utcnow()

    bucket = os.getenv('S3_BUCKET')
    key = _build_key(entity, dt)

    try:
        response = get_s3_client().get_object(Bucket=bucket, Key=key)
        body = response['Body'].read().decode('utf-8')
        records = [json.loads(line) for line in body.strip().splitlines()]
        logger.info(f"Loaded {len(records)} records from s3://{bucket}/{key}")
        return records
    except get_s3_client().exceptions.NoSuchKey:
        logger.warning(f"No file found at {key}")
        return None
    except Exception as e:
        logger.error(f"Load failed for {entity}: {e}")
        raise