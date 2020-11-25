from boto3.dynamodb.types import TypeSerializer
import json
from decimal import Decimal


"""
To convert data from JSON files to Dynamo JSON format
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


def replaceObjectName(data):
    data = data.replace('"M"', '"m"')
    data = data.replace('"L"', '"l"')
    data = data.replace('"S"', '"s"')
    data = data.replace('"N"', '"n"')
    return data


def converterToDynamodbFormat(data):
    typer = TypeSerializer()
    dynamodbJsonData = json.dumps(typer.serialize(data)["M"], default=encoder)
    return dynamodbJsonData


tables = ["ProductsData", "UsersData", "ReviewerData", "Orders", "ProductsData_search"]
for table in tables:
    print(table)
    with open("dynamo-export-rj-2/"+table+".json", "r") as f:
        data = json.load(f, parse_float=Decimal)
    if table == "ProductsData":
        batch_count = 0
        while batch_count < 10000:
            with open("converted_dynamo_2/"+table+"/"+table + "_" + str(batch_count) + ".json", "w+") as f:
                jsonBuilder = ""
                jsonBuilder += "{"
                jsonBuilder += "\""+table+"\": "
                jsonBuilder += ('[')
                table_dict = dict()
                table_dict[table] = []
                count = 0
                for e in data[batch_count: batch_count+20]:
                    dynamo_dict = dict()
                    dynamo_dict["PutRequest"] = dict()
                    dynamo_dict["PutRequest"]["Item"] = e
                    jsonBuilder += "{"
                    jsonBuilder += "\"PutRequest\": {"
                    jsonBuilder += "\"Item\": "
                    jsonBuilder += converterToDynamodbFormat(dynamo_dict["PutRequest"]["Item"])
                    jsonBuilder += "}"
                    jsonBuilder += "}"
                    count += 1
                    if count < len(data):
                        jsonBuilder += ","
                jsonBuilder += "]"
                jsonBuilder += "}"
                jsonBuilder = jsonBuilder[:len(jsonBuilder)-3] + jsonBuilder[len(jsonBuilder)-2:]
                f.write(jsonBuilder)
            batch_count += 20
    elif table == "UsersData":
        batch_count = 0
        while batch_count < 5000:
            with open("converted_dynamo_2/"+table+"/"+table + "_" + str(batch_count) + ".json", "w+") as f:
                jsonBuilder = ""
                jsonBuilder += "{"
                jsonBuilder += "\""+table+"\": "
                jsonBuilder += ('[')
                table_dict = dict()
                table_dict[table] = []
                count = 0
                for e in data[batch_count: batch_count+20]:
                    dynamo_dict = dict()
                    dynamo_dict["PutRequest"] = dict()
                    dynamo_dict["PutRequest"]["Item"] = e
                    jsonBuilder += "{"
                    jsonBuilder += "\"PutRequest\": {"
                    jsonBuilder += "\"Item\": "
                    jsonBuilder += converterToDynamodbFormat(dynamo_dict["PutRequest"]["Item"])
                    jsonBuilder += "}"
                    jsonBuilder += "}"
                    count += 1
                    if count < len(data):
                        jsonBuilder += ","
                        # f.write(',')
                jsonBuilder += "]"
                jsonBuilder += "}"
                jsonBuilder = jsonBuilder[:len(jsonBuilder)-3] + jsonBuilder[len(jsonBuilder)-2:]
                f.write(jsonBuilder)
            batch_count += 20
    elif table == "Orders":
        batch_count = 0
        while batch_count < 8000:
            with open("converted_dynamo_2/"+table+"/"+table + "_" + str(batch_count) + ".json", "w+") as f:
                jsonBuilder = ""
                jsonBuilder += "{"
                jsonBuilder += "\""+table+"\": "
                jsonBuilder += ('[')
                table_dict = dict()
                table_dict[table] = []
                count = 0
                for e in data[batch_count: batch_count+20]:
                    dynamo_dict = dict()
                    dynamo_dict["PutRequest"] = dict()
                    dynamo_dict["PutRequest"]["Item"] = e
                    jsonBuilder += "{"
                    jsonBuilder += "\"PutRequest\": {"
                    jsonBuilder += "\"Item\": "
                    jsonBuilder += converterToDynamodbFormat(dynamo_dict["PutRequest"]["Item"])
                    jsonBuilder += "}"
                    jsonBuilder += "}"
                    count += 1
                    if count < len(data):
                        jsonBuilder += ","
                jsonBuilder += "]"
                jsonBuilder += "}"
                jsonBuilder = jsonBuilder[:len(jsonBuilder)-3] + jsonBuilder[len(jsonBuilder)-2:]
                f.write(jsonBuilder)
            batch_count += 20