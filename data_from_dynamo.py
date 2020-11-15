import boto3
import json
from decimal import Decimal


class GenFakeFloat(float):
    def __init__(self, value):
        self._value = value

    def __repr__(self):
        return str(self._value)


def encoder(o):
    if isinstance(o, Decimal):
        return GenFakeFloat(o)
    raise TypeError(repr(o) + " is not JSON serializable")


def json_gen():
    tables = ["ProductsData", "UsersData", "ReviewerData", "Orders"]

    dynamodb = boto3.resource('dynamodb',
                              region_name='us-east-1',
                              aws_access_key_id='AKIAYUPXS4UXVNYE2PD2',
                              aws_secret_access_key='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'
                              )

    # dynamo_client = boto3.client('dynamodb',
    #                              region_name='us-east-1',
    #                              aws_access_key_id='AKIAYUPXS4UXVNYE2PD2',
    #                              aws_secret_access_key='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'
    #                              )

    for table in tables:
        # response_schema = dynamo_client.describe_table(
        #     TableName=table
        # )

        orders_table = dynamodb.Table(table)

        final_list = []
        response = orders_table.scan()
        for index in range(len(response["Items"])):
            final_list.append(response['Items'][index])

        with open("dynamo-export/" + table + ".json", "w") as f:
            json.dump(final_list, f, default=encoder)

        batch_write(table, dynamodb)


def batch_write(table, dynamodb):
    with open("dynamo-export/" + table + ".json", "r") as f:
        json_data = json.load(f, parse_float=Decimal)

    table_ob = dynamodb.Table(table)

    with table_ob.batch_writer() as batch:
        for item in json_data:
            batch.put_item(Item=item)


if __name__ == '__main__':
    json_gen()
