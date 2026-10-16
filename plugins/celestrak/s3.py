
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from celestrak import config


def client():
    return boto3.client(
        "s3",
        endpoint_url=config.S3_ENDPOINT_URL,
        aws_access_key_id=config.S3_ACCESS_KEY,
        aws_secret_access_key=config.S3_SECRET_KEY,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


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
