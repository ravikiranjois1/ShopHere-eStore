import json
import boto3
import pandas as pd
from typing import *
from decimal import Decimal


def get_resources() -> Tuple[boto3.resource, boto3.resource]:
    s3_resource = boto3.resource('s3',
                                 region_name='us-east-1',
                                 aws_access_key_id='AKIAYUPXS4UXVNYE2PD2',
                                 aws_secret_access_key='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'
                                 )

    return s3_resource


def converter(s3_resource):
    file_names = ['users.json', 'orders_data.json', 'meta_Office_Products.json',
                  'meta_Musical_Instruments.json', 'meta_Grocery_and_Gourmet_Food.json', 'meta_Baby.json']
    items_data = pd.Series()
    for file in file_names:
        first_object = s3_resource.Object(bucket_name='trials-project', key=file)
        body = first_object.get()['Body'].read().decode("utf-8")
        data = json.loads(body, parse_float=Decimal)
        if file == 'users.json':
            continue
            df = pd.json_normalize(data)
            df["USER_ID"] = df['userID']
            df["USER_ID"].to_csv("Users.csv", index=False)
        elif file == 'orders_data.json':
            continue
            df = []
            for entry in data:
                for item in entry["products_ordered"]:
                    df.append((entry["ordered_by"], item, entry["unixorderTime"]))
            pd.DataFrame(df, columns=["USER_ID", "ITEM_ID", "TIMESTAMP"]).to_csv("Interactions.csv", index=False)
            break
        else:
            df = pd.json_normalize(data)
            df["ITEM_ID"] = df["asin"]
            items_data = pd.concat([items_data, df["ITEM_ID"]], axis=0)
            print(items_data) #134838
    items_data.to_csv("Items.csv", index=False)


if __name__ == '__main__':
    s3_resource = get_resources()
    converter(s3_resource)
