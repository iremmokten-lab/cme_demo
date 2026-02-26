import boto3
from services.api.core.config import settings


s3 = boto3.client(
    "s3",
    endpoint_url=settings.S3_ENDPOINT,
    aws_access_key_id=settings.S3_ACCESS_KEY,
    aws_secret_access_key=settings.S3_SECRET_KEY
)


def upload_file(file_path: str, key: str):

    s3.upload_file(
        file_path,
        settings.S3_BUCKET,
        key
    )
