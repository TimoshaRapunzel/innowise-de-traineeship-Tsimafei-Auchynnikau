import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "test")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
    AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    LOCALSTACK_ENDPOINT = os.getenv("LOCALSTACK_ENDPOINT", "http://localhost:4566")
    S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "helsinki-bikes-data")
    DYNAMODB_TABLE_NAME = os.getenv("DYNAMODB_TABLE_NAME", "bikestation-metrics")

config = Config()
