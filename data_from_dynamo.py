import time
import boto3
import json
from decimal import Decimal

"""
To get data from DynamoDB and write to JSON files and also batch write the entirety of the data to 
Dynamo which may take about 14-15 hours to execute
Authors:
1. Pranjal Pandey
2. Ravikiran Jois
3. Shaivya Chandra
4. Suhas Choletti
"""


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
    tables = ["ProductsData", "UsersData", "Orders"]
    # tables = ["ProductsData"]

    dynamodb = boto3.resource('dynamodb',
                              region_name='us-east-1',
                              aws_access_key_id='AKIASDF5LFVUHMR6IU4T',
                              aws_secret_access_key='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'
                              )

    for table in tables:
        dynamoTable = dynamodb.Table(table)
        final_list = []
        search_list = []
        response = dynamoTable.scan()
        data = response.get('Items')
        while 'LastEvaluatedKey' in response:
            response = dynamoTable.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            data.extend(response['Items'])

        for index in range(len(data)):
            data[index]["asin"] = data[index].get("asin")
            data[index]["title"] = data[index].get("title")
            data[index]["description"] = data[index].get("description")
            if table == "ProductsData":
                prod_dict = dict()
                if data[index].get("asin") is None or data[index].get("title") is None \
                        or data[index].get("description") is None or len(data[index].get("asin")) == 0 \
                        or len(data[index].get("title")) == 0 or len(data[index].get("description")) == 0:
                    continue
                if "'" in data[index].get("description"):
                    print("here")
                    "'" in data[index].get("description") == "a"
                if data[index].get("asin") is not None:
                    prod_dict["asin"] = data[index].get("asin", "dummy")
                elif data[index].get("title") is not None:
                    prod_dict["title"] = data[index].get("title", "dummy")
                elif data[index].get("description") is not None:
                    prod_dict["description"] = data[index].get("description", "dummy")
                search_list.append(prod_dict)
            final_list.append(data[index])


        with open("dynamo-export-rj-2/" + table + ".json", "w") as f:
            json.dump(final_list, f, default=encoder)
        print(table, " done")


def batch_write():
    dynamodb = boto3.resource('dynamodb',
                              region_name='us-east-1',
                              aws_access_key_id='AKIASDF5LFVUHMR6IU4T',
                              aws_secret_access_key='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'
                              )

    tables = ["UsersData", "ReviewerData", "Orders", "ProductsData_search", "ProductsData"]
    start = time.time()
    for table in tables:
        print(table)
        with open("dynamo-export/" + table + ".json", "r") as f:
            json_data = json.load(f, parse_float=Decimal)

        table_ob = dynamodb.Table(table)

        # with table_ob.batch_writer() as batch:
        for item in json_data:
            table_ob.put_item(Item=item)
        print(time.time() - start)
    print("Final:", time.time()-start)


if __name__ == '__main__':
    json_gen()
    batch_write()