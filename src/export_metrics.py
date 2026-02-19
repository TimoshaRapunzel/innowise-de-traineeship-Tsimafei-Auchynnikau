import boto3
import csv
import os

def export_dynamo_to_csv():
    endpoint_url = os.environ.get('LOCALSTACK_ENDPOINT', 'http://localstack:4566')
    table_name = 'BikeMetrics'
    output_path = '/opt/airflow/data/helsinki_bikes_final_metrics.csv'

    print(f"Connecting to DynamoDB at {endpoint_url}...")
    dynamodb = boto3.resource('dynamodb', endpoint_url=endpoint_url, region_name='us-east-1')
    table = dynamodb.Table(table_name)

    print(f"Scanning table {table_name}...")
    response = table.scan()
    items = response.get('Items', [])

    if not items:
        print("No data found in DynamoDB.")
        return

    headers = ['StationName', 'DepartureCount', 'ReturnCount', 'ProcessedAt']

    print(f"Exporting {len(items)} rows to {output_path}...")
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction='ignore')
        writer.writeheader()
        for item in items:
            row = {
                'StationName': item.get('StationName'),
                'DepartureCount': int(item.get('DepartureCount', 0)),
                'ReturnCount': int(item.get('ReturnCount', 0)),
                'ProcessedAt': item.get('ProcessedAt')
            }
            writer.writerow(row)

    print("✅ Export finished successfully!")

if __name__ == "__main__":
    export_dynamo_to_csv()