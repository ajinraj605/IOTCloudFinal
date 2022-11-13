import boto3
from boto3.dynamodb.conditions import Key, Attr

# Get the service resource.
dynamodb = boto3.resource('dynamodb')

# Create the DynamoDB table.
table = dynamodb.Table('')
with table.batch_writer() as batch:
    for i in range(50):
        batch.put_item(
            Item={
                'account_type': 'anonymous',
                'deviceid': 'user' + str(i),
                'timestamp': 'unknown',
                'last_name': 'unknown'
            }
        )
items = response['Items']
print(items)