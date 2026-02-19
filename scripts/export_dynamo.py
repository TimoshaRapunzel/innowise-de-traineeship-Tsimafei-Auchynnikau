import boto3
import pandas as pd

dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:4566")
table = dynamodb.Table('BikeMetrics')

response = table.scan()
items = response['Items']

df = pd.DataFrame(items)
df.to_csv('final_dashboard_data.csv', index=False)
print("Exported for Tableau!")