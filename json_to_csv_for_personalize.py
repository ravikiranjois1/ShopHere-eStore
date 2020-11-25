import json
import boto3
import pandas as pd
from typing import *
from decimal import Decimal
import numpy as np
from operator import itemgetter

"""
For converting data from JSON format to suitable CSV files to load to AWS Personalize datasets
Authors:
1. Pranjal Pandey
2. Ravikiran Jois
3. Shaivya Chandra
4. Suhas Choletti
"""


def get_resources():
    s3_resource = boto3.resource('s3',
                                 region_name='us-east-1',
                                 aws_access_key_id='AKIASDF5LFVUHMR6IU4T',
                                 aws_secret_access_key='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'
                                 )

    return s3_resource


def converter(s3_resource):
    file_names = ['users.json', 'orders_data.json', 'meta_Office_Products.json',
                  'meta_Musical_Instruments.json', 'meta_Grocery_and_Gourmet_Food.json', 'meta_Baby.json']
    items_data = pd.Series(dtype=float)
    for file in file_names:
        first_object = s3_resource.Object(bucket_name='trials-project', key=file)
        body = first_object.get()['Body'].read().decode("utf-8")
        data = json.loads(body, parse_float=Decimal)
        if file == 'users.json':
            # continue
            df = pd.json_normalize(data)
            df["USER_ID"] = df['userID']
            df["AGE"] = pd.DataFrame(np.random.randint(12, 90, size=(len(df["USER_ID"]), 1)), columns=['AGE'])
            # final_df = pd.DataFrame([df["USER_ID"], df["AGE"]], columns=["USER_ID", "AGE"])
            final_df = pd.concat([df["USER_ID"], df["AGE"]], axis=1)
            final_df.to_csv("Users.csv", index=False)
        elif file == 'orders_data.json':
            # continue
            df = []
            for entry in data:
                for item in entry["products_ordered"]:
                    df.append((entry["ordered_by"], item, "ordered", 0, entry["unixorderTime"], "N/A", "N/A"))
            pd.DataFrame(df, columns=["USER_ID", "ITEM_ID", "EVENT_TYPE", "EVENT_VALUE", "TIMESTAMP", "IMPRESSION", "RECOMMENDATION_ID"]).to_csv("Interactions.csv", index=False)
        else:
            df = pd.json_normalize(data)
            df["ITEM_ID"] = df["asin"]
            df["GENRES"] = pd.Series(list(map(itemgetter(0), list(map(itemgetter(0), df["categories"])))))
            final_df = pd.concat([df["GENRES"], df["ITEM_ID"]], axis=1)
            items_data = pd.concat([items_data, final_df], axis=0)
    items_data[["ITEM_ID", "GENRES"]].to_csv("Items.csv", index=False)

    list_of_files = ["Items.csv", "Users.csv", "Interactions.csv"]
    for file in list_of_files:
        csv_object = s3_resource.Object(bucket_name='estore-personalize', key=file)
        csv_object.upload_file(file)


if __name__ == '__main__':
    s3_resource = get_resources()
    converter(s3_resource)