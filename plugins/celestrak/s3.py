
from botocore.exceptions import ClientError

from celestrak import config


def client():
    """boto3 S3 client of the Airflow connection s3_raw (endpoint, keys, path-style)."""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    return S3Hook(aws_conn_id=config.S3_CONN).get_conn()


def ensure_bucket(bucket: str = config.S3_RAW_BUCKET) -> None:
    s3 = client()
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        s3.create_bucket(Bucket=bucket)


def put(key: str, content: bytes, content_type: str, bucket: str = config.S3_RAW_BUCKET) -> None:
    ensure_bucket(bucket)
    client().put_object(Bucket=bucket, Key=key, Body=content, ContentType=content_type)


def get(key: str, bucket: str = config.S3_RAW_BUCKET) -> bytes:
    return client().get_object(Bucket=bucket, Key=key)["Body"].read()
