import boto3
import os
from botocore.exceptions import NoCredentialsError

class S3Manager:
    def __init__(self):
        self.s3 = boto3.client(
            's3',
            endpoint_url="http://localstack:4566",
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_DEFAULT_REGION')
        )
        self.bucket = os.getenv('S3_BUCKET_NAME')

    def upload_file(self, local_path: str, s3_key: str):
        try:
            self.s3.upload_file(local_path, self.bucket, s3_key)
            print(f"Uploaded {local_path} to {self.bucket}/{s3_key}")
        except NoCredentialsError:
            print("Credentials not available")

    def list_files(self, prefix: str):
        pass