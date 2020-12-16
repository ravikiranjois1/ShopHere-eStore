import json
import boto3


def lambda_handler(event, context):
    personalize_runtime = boto3.client("personalize-runtime")
    rec_response = personalize_runtime.get_recommendations(campaignArn=
                                                           "arn:aws:personalize:us-east-1:144278498664:campaign/personalize_campaign",
                                                           userId=event["pathParameters"]["id"])
    print(rec_response['recommendationId'])
    responeses = rec_response["itemList"][:6]
    ids = []
    for response in responeses:
        ids.append(response['itemId'])

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