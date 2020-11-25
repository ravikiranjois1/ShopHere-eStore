"""
To batch write data to the DynamoDB from the EC2 instance
Authors:
1. Pranjal Pandey
2. Ravikiran Jois
3. Shaivya Chandra
4. Suhas Choletti
"""

import boto3
from os import listdir
from os.path import isfile, join
import json
import os

resource = boto3.resource('dynamodb')

tables = ["UsersData", "ProductsData", "Orders"]

for table in tables:
    onlyfiles = [f for f in listdir(table + "") if
                 isfile(join(table, f))]

    for file in onlyfiles:
        with open(table + "/" + file, "r") as f:
            json_data = json.load(f)
            os.system("aws dynamodb batch-write-item --request-items file://" + table + "/" + file)