__authors = "PP, RJ, SC, SC"

"""
An e-Store Application with Recommendation system.
A program that creates data, fixes them and uploads them to S3 bucket and then from S3 to DynamoDB after data processing.

Authors:
Pranjal Pandey
Ravikiran Jois Yedur Prabhakar
Shaivya Chandra
Suhas Choletti
"""

import boto3
from io import StringIO
import csv
import pandas as pd
import json
import ast
from decimal import Decimal
from typing import *
import random
from datetime import datetime


def get_resources() -> Tuple[boto3.resource, boto3.resource]:
    s3_resource = boto3.resource('s3',
                                 region_name='us-east-1',
                                 aws_access_key_id='AKIAYUPXS4UXVNYE2PD2',
                                 aws_secret_access_key='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'
                                 )
    dynamodb_resource = boto3.resource('dynamodb',
                                       region_name='us-east-1',
                                       aws_access_key_id='AKIAYUPXS4UXVNYE2PD2',
                                       aws_secret_access_key='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'
                                       )

    return s3_resource, dynamodb_resource


def initial_fix_with_quotes(s3_resource, conversion_to_csv=False) -> None:
    """
    To upload a file to s3 bucket and convert to a pandas dataframe
    - The file here is a csv
    """

    # file_names = ['reviews_Musical_Instruments.json', 'reviews_Baby.json', 'reviews_Grocery_and_Gourmet_Food.json',
    #               'reviews_Office_Products.json', 'meta_Baby.json', 'meta_Grocery_and_Gourmet_Food.json',
    #               'meta_Musical_Instruments.json', 'meta_Office_Products.json']
    file_names = ['reviews_Musical_Instruments.json', 'reviews_Baby.json', 'reviews_Grocery_and_Gourmet_Food.json',
                  'reviews_Office_Products.json']
    for file in file_names:
        with open('Original_Data/' + file) as f:
            invalid_json = f.read()

        # Replace all ' with "
        valid_json = invalid_json.replace("'", '"')
        with open('Fixed_Data/' + file, 'w') as f:
            f.write(valid_json)

        if conversion_to_csv:
            first_object = s3_resource.Object(bucket_name='trials-project', key=file)
            first_object.upload_file(file)
            body = first_object.get()['Body'].read().decode("utf-8")
            f = StringIO(body)
            res_csv = []
            reader = csv.reader(f, delimiter=',')
            for row in reader:
                res_csv.append(row)
                # print(', '.join(row))
                df = pd.DataFrame.from_records(res_csv)

    return


def json_formatter(s3_resource) -> None:
    """
    To fix the json format and upload the files to s3 bucket
    """
    # file_names = ['reviews_Musical_Instruments.json', 'reviews_Baby.json', 'reviews_Grocery_and_Gourmet_Food.json',
    #               'reviews_Office_Products.json', 'meta_Baby.json', 'meta_Grocery_and_Gourmet_Food.json',
    #               'meta_Musical_Instruments.json', 'meta_Office_Products.json']
    file_names = ['reviews_Musical_Instruments.json', 'reviews_Baby.json', 'reviews_Grocery_and_Gourmet_Food.json',
                  'reviews_Office_Products.json']
    for file in file_names:
        file_read_from = 'Original_Data/' + file
        file_write_to = 'Fixed_Data/' + file
        fr = open(file_read_from)
        fw = open(file_write_to, 'w')
        fw.write('[')
        all_lines = fr.readlines()
        json_dat = json.dumps(ast.literal_eval(all_lines[0]))
        dict_dat = json.loads(json_dat)
        json.dump(dict_dat, fw)
        for line in all_lines[1:]:
            fw.write(",\n")
            json_dat = json.dumps(ast.literal_eval(line))
            dict_dat = json.loads(json_dat)
            json.dump(dict_dat, fw)
        fw.write(']')
        fw.close()
        fr.close()

        '''To upload the file to s3 bucket'''
        first_object = s3_resource.Object(bucket_name='trials-project', key=file)
        first_object.upload_file(file_write_to)

        # users = open('Fixed_Data/users.json')
        # first_object = s3_resource.Object(bucket_name='trials-project', key=users)
        # first_object.upload_file(file_write_to)
        #
        # orders = open('Fixed_Data/orders.json')
        # first_object = s3_resource.Object(bucket_name='trials-project', key=orders)
        # first_object.upload_file(file_write_to)

    return


def create_users(s3_resource) -> None:
    """
    To get JSON from s3 bucket and generate user data and product data in JSON format
    """
    file_names = ['Fixed_Data/reviews_Musical_Instruments.json', 'Fixed_Data/reviews_Baby.json',
                  'Fixed_Data/reviews_Grocery_and_Gourmet_Food.json', 'Fixed_Data/reviews_Office_Products.json']

    user_names_set = set()
    tweets = []
    print('creating users data')
    for file_name in file_names:
        with open('Fixed_Data/'+file_name, 'r') as f:
            json_data = json.load(f)
        # body = first_object.get()['Body'].read().decode('UTF-8')
        # json_data = json.loads(body)
        for entry in json_data:
            if entry["reviewerID"] not in user_names_set:
                user_names_set.add(entry["reviewerID"])
                user_info = dict()
                user_info["userID"] = entry["reviewerID"]
                if "reviewerName" not in entry:
                    user_info["user_name"] = "Name " + entry["reviewerID"]
                else:
                    user_info["user_name"] = entry["reviewerName"]
                user_info["password"] = entry["reviewerID"]
                user_info["firstname"] = "first_" + entry["reviewerID"]
                user_info["lastname"] = "last_" + entry["reviewerID"]
                tweets.append(user_info)
        print(file_name + "...")

    with open("Fixed_Data/users.json", "w") as f:
        json.dump(tweets, f)

    users = 'Fixed_Data/users.json'
    first_object = s3_resource.Object(bucket_name='trials-project', key='users.json')
    first_object.upload_file(users)

    return


def create_order_data(s3_resource) -> None:
    """
    To get JSON from s3 bucket and generate order data in JSON format
    """
    file_names = ['Fixed_Data/reviews_Musical_Instruments.json', 'Fixed_Data/reviews_Baby.json',
                  'Fixed_Data/reviews_Grocery_and_Gourmet_Food.json', 'Fixed_Data/reviews_Office_Products.json']

    tweets = []
    print('creating order data')
    for file_name in file_names:
        with open(file_name, 'r') as f:
            json_data = json.load(f)
        # json_data = json.loads(body)
        order_id = 0
        for entry in json_data:
            order_info = dict()
            order_info['order_id'] = 'OD' + str(order_id)
            order_info['unixorderTime'] = entry['unixReviewTime'] - (random.randint(3, 5) * 86400)
            order_info['order_time'] = datetime.utcfromtimestamp(order_info['unixorderTime']).strftime(
                '%Y-%m-%d %H:%M:%S')
            products = []
            for product_find in json_data:
                if product_find['reviewerID'] == entry['reviewerID'] and entry['unixReviewTime'] == \
                        product_find['unixReviewTime']:
                    products.append(product_find['asin'])
            order_info['ordered_by'] = entry['reviewerID']
            order_info['products_ordered'] = {}
            for product in products:
                order_info['products_ordered'][product] = random.randint(1, 10)
            order_id += 1
            tweets.append(order_info)
        print(file_name + "...")

    with open("Fixed_Data/orders_data.json", "w") as f:
        json.dump(tweets, f)

    orders = 'Fixed_Data/orders_data.json'
    first_object = s3_resource.Object(bucket_name='trials-project', key='orders_data.json')
    first_object.upload_file(orders)

    return


def put_data_to_DynamoDB(s3_resource, dynamodb) -> None:
    """
    To create and upload table and entries (JSON) in DynamoDB respectively
    """
    reviewer_filenames = ['reviews_Musical_Instruments.json', 'reviews_Baby.json',
                          'reviews_Grocery_and_Gourmet_Food.json', 'reviews_Office_Products.json']
    products_filenames = ['meta_Baby.json', 'meta_Grocery_and_Gourmet_Food.json',
                          'meta_Musical_Instruments.json', 'meta_Office_Products.json']

    # for file in reviewer_filenames:
    #     first_object = s3_resource.Object(bucket_name='trials-project', key=file)
    #     body = first_object.get()['Body'].read().decode('UTF-8')
    #     reviews = json.loads(body, parse_float=Decimal)
    #     table = dynamodb.Table('ReviewerData')
    #     for review in reviews:
    #         table.put_item(Item=review)
    #     print(file+' - Done - ReviewerData')
    #
    # for file in products_filenames:
    #     first_object = s3_resource.Object(bucket_name='trials-project', key=file)
    #     body = first_object.get()['Body'].read().decode('UTF-8')
    #     products = json.loads(body, parse_float=Decimal)
    #     table = dynamodb.Table('ProductsData')
    #     for product in products:
    #         table.put_item(Item=product)
    #     print(file + ' - Done - ProductsData')
    #
    # first_object = s3_resource.Object(bucket_name='trials-project', key='users.json')
    # body = first_object.get()['Body'].read().decode('UTF-8')
    # users = json.loads(body, parse_float=Decimal)
    # table = dynamodb.Table('UsersData')
    # for user in users:
    #     table.put_item(Item=user)
    # print(' - Done - UsersData')

    first_object = s3_resource.Object(bucket_name='trials-project', key='orders_data.json')
    body = first_object.get()['Body'].read().decode('UTF-8')
    reviews = json.loads(body, parse_float=Decimal)
    table = dynamodb.Table('Orders')
    for review in reviews:
        table.put_item(Item=review)
    print(' - Done - Orders')

    return None


if __name__ == '__main__':
    s3_resource, dynamoDB_resource = get_resources()
    initial_fix_with_quotes(s3_resource)
    json_formatter(s3_resource)
    create_users(s3_resource)
    create_order_data(s3_resource)
    put_data_to_DynamoDB(s3_resource, dynamoDB_resource)
