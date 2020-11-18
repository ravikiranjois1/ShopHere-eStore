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
    # tables = ["ProductsData"]

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

        dynamoTable = dynamodb.Table(table)

        final_list = []
        search_list = []
        response = dynamoTable.scan()
        data = response.get('Items')
        while 'LastEvaluatedKey' in response:
            response = dynamoTable.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            data.extend(response['Items'])

        for index in range(len(data)):
            data[index]["asin"] = data[index].get("asin", "").strip()
            data[index]["title"] = data[index].get("title", "").strip()
            data[index]["description"] = data[index].get("description", "").strip()
            if table == "ProductsData":
                prod_dict = dict()
                if data[index].get("asin") is None or data[index].get("title") is None \
                        or data[index].get("description") is None or len(data[index].get("asin")) == 0 \
                        or len(data[index].get("title")) == 0 or len(data[index].get("description")) == 0:
                    continue
                if "'" in data[index].get("description"):
                    print("here")
                    "'" in data[index].get("description") == "a"
                prod_dict["asin"] = data[index].get("asin", None)
                prod_dict["title"] = data[index].get("title", None)
                prod_dict["description"] = data[index].get("description", None)
                search_list.append(prod_dict)
            final_list.append(data[index])

        if table == "ProductsData":
            with open("dynamo-export/" + table + "_search.json", "w") as f:
                json.dump(search_list, f, default=encoder)
            # batch_write(table + "_search", dynamodb)

        with open("dynamo-export/" + table + ".json", "w") as f:
            json.dump(final_list, f, default=encoder)

        # batch_write(table, dynamodb)


def batch_write(table, dynamodb):
    with open("dynamo-export/" + table + ".json", "r") as f:
        json_data = json.load(f, parse_float=Decimal)

    table_ob = dynamodb.Table(table)

    with table_ob.batch_writer() as batch:
        for item in json_data:
            batch.put_item(Item=item)


# table = "ProductES"
# dynamodb = boto3.resource('dynamodb',
#                               region_name='us-east-1',
#                               aws_access_key_id='AKIAYUPXS4UXVNYE2PD2',
#                               aws_secret_access_key='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'
#                               )
# batch_write(table, dynamodb)

if __name__ == '__main__':
    json_gen()
