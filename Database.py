# Imports MongoClient for base level access to the local MongoDB
import boto3
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb')


class Database:

    #This function used to get data data from the dynamoDB
    def get_data(table_name, dev_id):
        table = dynamodb.Table(table_name)
        response = table.query(KeyConditionExpression=Key('deviceid').eq(dev_id))
        table_data = response['Items']
        return table_data

    #This function used to upload the data to the Agg_Table in dynamoDB
    def upload_agg_data(Final_list):
        # Create the DynamoDB table.
        table = dynamodb.Table('Agg_Table')
        for k in range(0, len(Final_list)):
            table.put_item(
                Item={
                    'deviceid': Final_list[k]['deviceid'],
                    'timestamp': Final_list[k]['timestamp'],
                    'datatype': Final_list[k]['datatype'],
                    'Average': Final_list[k]['value'],
                    'minimum': Final_list[k]['min'],
                    'maximum': Final_list[k]['max'],
                }
            )

    # This function used to upload the data to the Anomaly_Table in dynamoDB
    def upload_anomaly_data(final_data):
        Readings = {
            'deviceid': final_data['deviceid'],
            'timestamp': final_data['timestamp'],
            'datatype': final_data['datatype'],
            'Rule': final_data['Rule'],
            'Rule_Alert': final_data['Alert']
        }

        # Create the DynamoDB table.
        table = dynamodb.Table('Anomaly_Table')
        table.put_item(Item=Readings)

    #This function is used to create the table based on given table name
    def create_table(name):
        table = dynamodb.create_table(
            TableName=name,
            KeySchema=[
                {
                    'AttributeName': 'deviceid',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'timestamp',
                    'KeyType': 'RANGE'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'deviceid',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'timestamp',
                    'AttributeType': 'S'
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )

        # Wait until the table exists.
        table.wait_until_exists()

        # Print out some data about the table.
        print(name, ' created')
