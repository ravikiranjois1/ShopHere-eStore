import boto3
from boto3.dynamodb.conditions import Key
import simplejson as json


def lambda_handler(event, context):
    # TODO implement
    dynamodb = boto3.resource('dynamodb')
    productsTable = dynamodb.Table('ProductsData')
    productID = event["pathParameters"]["id"]
    productResponse = productsTable.query(
        KeyConditionExpression=Key('asin').eq(productID)
    )

    reviewResponse = productsTable.scan(FilterExpression=Key('asin').eq(productID))

    productInfo = {
        "productData": productResponse,
        "reviewData": reviewResponse
    }

    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
            'Access-Control-Allow-Credentials': True,
            'Content-Type': 'application/json'
        },
        'body': json.dumps(productInfo),
        "isBase64Encoded": False
    }