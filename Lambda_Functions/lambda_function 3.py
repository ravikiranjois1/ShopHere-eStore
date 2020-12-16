import json
import boto3


def lambda_handler(event, context):
    bucket = "estore-rekognition"
    photo = event["pathParameters"]["id"] + ".jpg"

    client = boto3.client('rekognition')

    # process using S3 object

    response = client.detect_labels(Image={'S3Object': {'Bucket': bucket, 'Name': photo}},
                                    MaxLabels=1,
                                    MinConfidence=30)

    # Get the custom labels
    label = response['Labels'][0]["Name"]

    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
            'Access-Control-Allow-Credentials': True,
            'Content-Type': 'application/json'
        },
        'body': json.dumps(label)
    }