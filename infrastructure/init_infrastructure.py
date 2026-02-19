import boto3
import zipfile
import os
import json
import time

ENDPOINT_URL = os.environ.get("LOCALSTACK_ENDPOINT", "http://localstack:4566")
REGION = "us-east-1"
BUCKET_NAME = "helsinki-bikes-data"
TABLE_NAME = "BikeMetrics"
TOPIC_NAME = "NewMetricsUploadedTopic"
LAMBDA_NAME = "ProcessMetrics"
ROLE_NAME = "lambda-ex"

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(CURRENT_DIR)

LAMBDA_SRC_FILE = os.path.join(BASE_DIR, "src", "lambda_function.py")
ZIP_FILE_PATH = "/tmp/lambda_function.zip"

print(f"DEBUG: Base Dir: {BASE_DIR}")
print(f"DEBUG: Looking for Lambda at: {LAMBDA_SRC_FILE}")

s3 = boto3.client("s3", endpoint_url=ENDPOINT_URL, region_name=REGION)
dynamodb = boto3.client("dynamodb", endpoint_url=ENDPOINT_URL, region_name=REGION)
sns = boto3.client("sns", endpoint_url=ENDPOINT_URL, region_name=REGION)
lam = boto3.client("lambda", endpoint_url=ENDPOINT_URL, region_name=REGION)
iam = boto3.client("iam", endpoint_url=ENDPOINT_URL, region_name=REGION)

def setup_infrastructure():
    print("--- STARTING INFRASTRUCTURE SETUP ---")

    try:
        s3.create_bucket(Bucket=BUCKET_NAME)
        print(f"✅ Bucket '{BUCKET_NAME}' created.")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        print(f"ℹ️ Bucket '{BUCKET_NAME}' already exists.")

    try:
        dynamodb.create_table(
            TableName=TABLE_NAME,
            KeySchema=[{'AttributeName': 'StationDate', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'StationDate', 'AttributeType': 'S'}],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        print(f"✅ Table '{TABLE_NAME}' created.")
    except dynamodb.exceptions.ResourceInUseException:
        print(f"ℹ️ Table '{TABLE_NAME}' already exists.")

    sns_response = sns.create_topic(Name=TOPIC_NAME)
    topic_arn = sns_response['TopicArn']
    print(f"✅ SNS Topic created: {topic_arn}")

    try:
        iam.create_role(
            RoleName=ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps({
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]
            })
        )
        print(f"✅ IAM Role '{ROLE_NAME}' created.")
    except iam.exceptions.EntityAlreadyExistsException:
        print(f"ℹ️ IAM Role '{ROLE_NAME}' already exists.")

    time.sleep(1) 

    if not os.path.exists(LAMBDA_SRC_FILE):
        raise FileNotFoundError(f"CRITICAL: Lambda file not found at {LAMBDA_SRC_FILE}")

    print("📦 Zipping Lambda code...")
    with zipfile.ZipFile(ZIP_FILE_PATH, 'w') as z:
        z.write(LAMBDA_SRC_FILE, arcname="lambda_function.py")

    with open(ZIP_FILE_PATH, 'rb') as f:
        zipped_code = f.read()

    try:
        lam.delete_function(FunctionName=LAMBDA_NAME)
        print(f"♻️ Old function '{LAMBDA_NAME}' deleted.")
    except:
        pass

    print(f"🚀 Deploying Lambda '{LAMBDA_NAME}'...")
    role_arn = f"arn:aws:iam::000000000000:role/{ROLE_NAME}"
    
    lambda_response = lam.create_function(
        FunctionName=LAMBDA_NAME,
        Runtime='python3.9',
        Role=role_arn,
        Handler='lambda_function.lambda_handler',
        Code={'ZipFile': zipped_code},
        Environment={
            'Variables': {
                'LOCALSTACK_ENDPOINT': ENDPOINT_URL,
                'DYNAMODB_TABLE_METRICS': TABLE_NAME
            }
        }
    )
    lambda_arn = lambda_response['FunctionArn']

    print("🔗 Subscribing Lambda to SNS...")
    sns.subscribe(TopicArn=topic_arn, Protocol='lambda', Endpoint=lambda_arn)
    
    lam.add_permission(
        FunctionName=LAMBDA_NAME,
        StatementId='sns-invoke',
        Action='lambda:InvokeFunction',
        Principal='sns.amazonaws.com',
        SourceArn=topic_arn
    )

    print("🔔 Configuring S3 Notifications...")
    s3.put_bucket_notification_configuration(
        Bucket=BUCKET_NAME,
        NotificationConfiguration={
            'TopicConfigurations': [
                {
                    'TopicArn': topic_arn,
                    'Events': ['s3:ObjectCreated:*'],
                    'Filter': {
                        'Key': {
                            'FilterRules': [
                                {'Name': 'prefix', 'Value': 'processed/'},
                                {'Name': 'suffix', 'Value': '.json'}
                            ]
                        }
                    }
                }
            ]
        }
    )

    if os.path.exists(ZIP_FILE_PATH):
        os.remove(ZIP_FILE_PATH)

    print("\n🎉 SUCCESS! Infrastructure ready: S3 -> SNS -> Lambda -> DynamoDB")

if __name__ == "__main__":
    setup_infrastructure()
