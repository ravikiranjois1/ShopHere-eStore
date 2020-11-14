import boto3
import json
from decimal import Decimal

dynamodb = boto3.resource('dynamodb',
                          region_name='us-east-1',
                          aws_access_key_id='AKIAYUPXS4UXVNYE2PD2',
                          aws_secret_access_key='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'
                          )


class genfakefloat(float):
    def __init__(self, value):
        self._value = value

    def __repr__(self):
        return str(self._value)


def encoder(o):
    if isinstance(o, Decimal):
        return genfakefloat(o)
    raise TypeError(repr(o) + " is not JSON serializable")


tables = ["Orders", "ProductsData", "UsersData", "ReviewerData"]

for table in tables:
    orders_table = dynamodb.Table(table)

    final_dict = dict()
    final_dict[table] = []

    response = orders_table.scan()
    for index in range(len(response["Items"])):
        entry = {"PutRequest": {"Item": response['Items'][index]}}
        final_dict[table].append(entry)

    with open("dynamo-export/"+table+".json", "w") as f:
        json.dump(final_dict, f, default=encoder)