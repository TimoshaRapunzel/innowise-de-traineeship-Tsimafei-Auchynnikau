import boto3
import json
import io
import os
import time

ENDPOINT_URL = os.environ.get('LOCALSTACK_ENDPOINT', 'http://localstack:4566')
TABLE_NAME = os.environ.get('DYNAMODB_TABLE_METRICS', 'BikeMetrics')

s3_client = boto3.client('s3', endpoint_url=ENDPOINT_URL)
dynamodb = boto3.resource('dynamodb', endpoint_url=ENDPOINT_URL)
table = dynamodb.Table(TABLE_NAME)

def lambda_handler(event, context):
    print("--- LAMBDA INNOWISE-STYLE START ---")
    
    try:
        for record in event.get('Records', []):
            if 'Sns' in record:
                s3_event = json.loads(record['Sns']['Message'])
            else:
                s3_event = record

            if 'Records' not in s3_event:
                print("DEBUG: No S3 records found in this event part. Skipping.")
                continue

            for s3_record in s3_event['Records']:
                bucket = s3_record['s3']['bucket']['name']
                key = s3_record['s3']['object']['key']
                
                print(f"ACTION: Reading file s3://{bucket}/{key}")

                if not key.endswith('.json') or '_SUCCESS' in key:
                    continue

                response = s3_client.get_object(Bucket=bucket, Key=key)
                content = response['Body'].read().decode('utf-8')

                items_to_save = []
                for line in content.splitlines():
                    if not line.strip():
                        continue
                    
                    data = json.loads(line)
                    
                    station_name = data.get('station_name', 'Unknown')
                    dep_count = data.get('departure_count', 0)
                    ret_count = data.get('return_count', 0)

                    items_to_save.append({
                        'StationDate': f"{station_name}_{int(time.time() * 1000)}",
                        'StationName': station_name,
                        'DepartureCount': int(dep_count),
                        'ReturnCount': int(ret_count),
                        'ProcessedAt': time.strftime('%Y-%m-%d %H:%M:%S')
                    })

                if items_to_save:
                    print(f"INFO: Saving {len(items_to_save)} metrics for stations...")
                    with table.batch_writer() as batch:
                        for item in items_to_save:
                            batch.put_item(Item=item)
                    print(f"SUCCESS: {key} integrated into DynamoDB.")

    except Exception as e:
        print(f"CRITICAL ERROR: {str(e)}")
        raise e

    return {"status": "success", "processed_files_count": len(event.get('Records', []))}
