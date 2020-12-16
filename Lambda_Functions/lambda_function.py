import boto3
from boto3.dynamodb.conditions import Key
import json
from datetime import datetime
import time
from random import randint


def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    ordersTable = dynamodb.Table('Orders')

    products_ordered = dict()
    event = json.loads(event["body"])
    for products in event["products"]:
        products_ordered[products] = randint(1, 10)
    unixtime = int(time.time())
    now = datetime.now()
    time_now = now.strftime("%d/%m/%Y %H:%M:%S")
    order_id = 'OD' + str(unixtime)
    PutOrdersResponse = ordersTable.put_item(
        Item={
            'order_id': order_id,
            'order_time': time_now,
            'ordered_by': event["id"],
            'products_ordered': products_ordered,
            'unixorderTime': unixtime
        }
    )
    # print(order_id)

    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
            'Access-Control-Allow-Credentials': True,
            'Content-Type': 'application/json'
        },
        'body': order_id,
        "isBase64Encoded": False
    }