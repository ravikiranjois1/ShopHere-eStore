import boto3
from boto3.dynamodb.conditions import Key
import simplejson as json

def lambda_handler(event, context):
  # TODO implement
  dynamodb = boto3.resource('dynamodb')
  ordersTable = dynamodb.Table('Orders')
  userID = event["pathParameters"]["id"]
  ordersResponse = ordersTable.scan(
      FilterExpression=Key('ordered_by').eq(userID)
  )
  
  data = ordersResponse['Items']
  
  while 'LastEvaluatedKey' in ordersResponse:
      ordersResponse = ordersTable.scan(
          FilterExpression=Key('ordered_by').eq(userID),
          ExclusiveStartKey=ordersResponse['LastEvaluatedKey']
      )
      data.extend(ordersResponse['Items'])
  
  sortedData = sorted(data, key=lambda k: k["unixorderTime"],reverse = True) 
    
  
  return {
    'statusCode': 200,
    'headers': {
            'Access-Control-Allow-Origin' : '*',
            'Access-Control-Allow-Headers':'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
            'Access-Control-Allow-Credentials' : True,
            'Content-Type': 'application/json'
        },
    'body': json.dumps(sortedData[:10]),
    "isBase64Encoded": False
  }