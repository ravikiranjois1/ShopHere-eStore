import json
import boto3
from boto3.dynamodb.conditions import Key


def lambda_handler(event, context):
    # define the handler function that the Lambda service will use as an entry point
    # return {
    #     "statusCode": 200,
    #     "body": json.dumps(event)
    # }
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('UsersData')
    userID = event["userID"]
    response = table.query(
        KeyConditionExpression=Key('userID').eq(userID)
    )
    if len(response["Items"]) == 0:
        return {
            'statusCode': 404,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                'Access-Control-Allow-Credentials': True,
                'Content-Type': 'application/json'
            },
            'body': json.dumps("Username not found!")
        }
    else:
        password = response["Items"][0]["password"]
        if password == event["password"]:
            return {
                'statusCode': 200,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                    'Access-Control-Allow-Credentials': True,
                    'Content-Type': 'application/json'
                },
                'body': json.dumps("Username Exists")
            }
        else:
            return {
                'statusCode': 401,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
                    'Access-Control-Allow-Credentials': True,
                    'Content-Type': 'application/json'
                },
                'body': json.dumps("Password Incorrect!")
            }
