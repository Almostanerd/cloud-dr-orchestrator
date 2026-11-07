import os
import time
import boto3
from botocore.exceptions import ClientError

LOCALSTACK_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localstack:4566")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

s3_client = boto3.client(
    "s3",
    endpoint_url=LOCALSTACK_ENDPOINT,
    region_name=AWS_REGION,
    aws_access_key_id="test",
    aws_secret_access_key="test",
)

dynamodb_client = boto3.client(
    "dynamodb",
    endpoint_url=LOCALSTACK_ENDPOINT,
    region_name=AWS_REGION,
    aws_access_key_id="test",
    aws_secret_access_key="test",
)

HOT_BUCKET = "hot-snapshots-tier"
COLD_BUCKET = "cold-archive-tier"
MULTI_CLOUD_REPLICA_BUCKET = "gcp-cloud-storage-replica"
TABLE_NAME = "BackupMetadata"


def init_aws_infrastructure(max_retries: int = 10, delay: int = 2):
    """Provisions S3 buckets (including multi-cloud replica) and DynamoDB table."""
    for attempt in range(max_retries):
        try:
            # 1. Multi-tier & Multi-cloud Mock Buckets
            for bucket in [HOT_BUCKET, COLD_BUCKET, MULTI_CLOUD_REPLICA_BUCKET]:
                try:
                    s3_client.head_bucket(Bucket=bucket)
                except ClientError:
                    s3_client.create_bucket(Bucket=bucket)

            # 2. DynamoDB Table for Telemetry
            try:
                dynamodb_client.describe_table(TableName=TABLE_NAME)
            except ClientError:
                dynamodb_client.create_table(
                    TableName=TABLE_NAME,
                    KeySchema=[
                        {"AttributeName": "PK", "KeyType": "HASH"},
                        {"AttributeName": "Timestamp", "KeyType": "RANGE"}
                    ],
                    AttributeDefinitions=[
                        {"AttributeName": "PK", "AttributeType": "S"},
                        {"AttributeName": "Timestamp", "AttributeType": "N"}
                    ],
                    BillingMode="PAY_PER_REQUEST"
                )
            print("[Bootstrap] Multi-cloud and AWS infrastructure initialized.")
            return
        except Exception as e:
            print(f"[Bootstrap] Attempt {attempt + 1}/{max_retries} failed: {e}. Retrying in {delay}s...")
            time.sleep(delay)

    raise RuntimeError("Failed to connect to AWS LocalStack infrastructure.")