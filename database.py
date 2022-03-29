import boto3

from botocore.exceptions import ClientError


class Database:
    def __init__(self):
        self.client = boto3.client("dynamodb")
    
    def get_db_entries(self, TableName, city, ProjecionExpression=None):
        try:
            if ProjecionExpression:
                results = self.client.scan(
                    TableName=TableName,
                    FilterExpression="city = :val",
                    ExpressionAttributeValues={
                        ":val": {
                            "S": city,
                        },
                    },    
                    ProjectionExpression=ProjecionExpression,
                )
            else:
                results = self.client.scan(
                    TableName=TableName,
                    FilterExpression="city = :val",
                    ExpressionAttributeValues={
                        ":val": {
                            "S": city,
                        },
                    },    
                )
            return results['Items']
        except ClientError as e:
            print(e.response['Error']['Message'])

    def delete_from_db(self, TableName, Type, Item_Key, Value):
        try:
            self.client.delete_item(
                TableName=TableName,
                Key={
                    Item_Key: {
                        Type: Value
                    }
                }
            )
        except ClientError as e:
            print(e.response['Error']['Message'])

    def update_database(self, TableName, Item):
        try:
            self.client.put_item(
                TableName=TableName,
                Item=Item
            )
        except ClientError as e:
            print(e.response['Error']['Message'])