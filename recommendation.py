import boto3
import pandas as pd
import time


"""
To set up AWS Personalize and create the necessary components and datasets
Authors:
1. Pranjal Pandey
2. Ravikiran Jois
3. Shaivya Chandra
4. Suhas Choletti
"""


def verifying_env():
    personalize = boto3.client('personalize',
                               region_name='us-east-1',
                               aws_access_key_id='AKIASDF5LFVUHMR6IU4T',
                               aws_secret_access_key='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'
                               )
    # response = personalize.list_recipes()
    # for recipe in response['recipes']:
    #     print(recipe)
    return personalize


def creating_schema(personalize_client):
    list_of_schema = ["schema/interactions.json", "schema/items.json", "schema/users.json"]
    schema_names = ["Interactions", "Items", "Users"]
    dict_of_ARNs = dict()
    for index, schema_file in enumerate(list_of_schema):
        with open(schema_file, "r") as f:
            createSchemaResponse = personalize_client.create_schema(
                name=schema_names[index],
                schema=f.read()
            )

        schema_arn = createSchemaResponse['schemaArn']
        dict_of_ARNs[schema_names[index]] = schema_arn
    return dict_of_ARNs


def preparing_and_importing_data(personalize, dict_of_ARNs):
    # dict_of_ARNs = {'Interactions': 'arn:aws:personalize:us-east-1:593761592623:schema/Interactions',
    #                 'Items': 'arn:aws:personalize:us-east-1:593761592623:schema/Items',
    #                 'Users': 'arn:aws:personalize:us-east-1:593761592623:schema/Users'}
    response = personalize.create_dataset_group(name='EStoreGroup')
    dsg_arn = response['datasetGroupArn']

    # To get the description of the dataset group
    # description = personalize.describe_dataset_group(datasetGroupArn=dsg_arn)['datasetGroup']
    return dict_of_ARNs, dsg_arn


def create_datasets(personalize, dict_of_ARNs, dsg_arn):
    # To create dataset:
    response_dict = dict()
    for arn in dict_of_ARNs:
        response = personalize.create_dataset(
            name=arn + '-dataset',
            schemaArn=dict_of_ARNs[arn],
            datasetGroupArn=dsg_arn,
            datasetType=arn
        )

        # print('Dataset Arn: ' + response['datasetArn'])
        response_dict[arn] = response['datasetArn']
    return response_dict


def bulk_import(personalize, dataset_arn):
    response_dict = dict()
    for arn in dataset_arn:
        print(arn)
        response = personalize.create_dataset_import_job(
            jobName=arn + '_jobs',
            datasetArn=dataset_arn[arn],
            dataSource={'dataLocation': 's3://estore-personalize-1/' + arn + '.csv'},
            roleArn='arn:aws:iam::144278498664:role/PersonalizeRole'
        )

        dsij_arn = response['datasetImportJobArn']

        print('Dataset Import Job arn: ' + dsij_arn)

        description = personalize.describe_dataset_import_job(
            datasetImportJobArn=dsij_arn)['datasetImportJob']

        print('Name: ' + description['jobName'])
        print('ARN: ' + description['datasetImportJobArn'])
        print('Status: ' + description['status'])
        response_dict[arn] = response['datasetImportJobArn']

    return response_dict


if __name__ == '__main__':
    personalize_client = verifying_env()
    # print(personalize_client.list_schemas())
    dict_of_ARNs = creating_schema(personalize_client)
    dict_of_ARNs, dsg_arn = preparing_and_importing_data(personalize_client, dict_of_ARNs)
    time.sleep(45)
    dataset_arn = create_datasets(personalize_client, dict_of_ARNs, dsg_arn)
    print('check')
    # dataset_arn = {'Interactions': 'arn:aws:personalize:us-east-1:144278498664:schema/Interactions',
    #                'Items': 'arn:aws:personalize:us-east-1:144278498664:schema/Items',
    #                'Users': 'arn:aws:personalize:us-east-1:144278498664:schema/Users'}

    bulk_import(personalize_client, dataset_arn)
    # To delete schemas:
    # response = personalize_client.delete_schema(
    #      schemaArn='arn:aws:personalize:us-east-1:144278498664:schema/Interactions'
    #  )
    # response = personalize_client.delete_schema(
    #      schemaArn='arn:aws:personalize:us-east-1:144278498664:schema/Items'
    # )
    # response = personalize_client.delete_schema(
    #      schemaArn='arn:aws:personalize:us-east-1:144278498664:schema/Users'
    #   )
