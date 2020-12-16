import json
import boto3
# import boto
from boto3.dynamodb.conditions import Key
from decimal import Decimal
from boto3.dynamodb.conditions import Contains


class GenFakeFloat(float):
    def __init__(self, value):
        self._value = value

    def __repr__(self):
        return str(self._value)


def encoder(o):
    if isinstance(o, Decimal):
        return GenFakeFloat(o)
    raise TypeError(repr(o) + " is not JSON serializable")


def lambda_handler(event, context):
    client = boto3.client('cloudsearchdomain',
                          endpoint_url='http://search-cs-products-k5fendgdenl4wkj2y7nzdd3ruq.us-east-1.cloudsearch.amazonaws.com'
                          )
    responses = client.search(
        query=event["pathParameters"]["id"],
        size=10
    )
    ids = []
    for response in responses["hits"]["hit"]:
        ids.append(response["id"])

    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
            'Access-Control-Allow-Credentials': True,
            'Content-Type': 'application/json'
        },
        'body': json.dumps(ids),
        "isBase64Encoded": False
    }
