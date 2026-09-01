"""Create the tidaline-lakehouse bucket in MinIO on startup."""
import boto3
from botocore.exceptions import ClientError

s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    region_name="us-east-1",
)

try:
    s3.create_bucket(Bucket="tidaline-lakehouse")
    print("Bucket 'tidaline-lakehouse' created successfully!")
except ClientError as e:
    if e.response["Error"]["Code"] in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
        print("Bucket 'tidaline-lakehouse' already exists, skipping.")
    else:
        raise
