import boto3
import time

"""
To train the data and create campaign on AWS Personalize
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
    response = personalize.list_recipes()
    dataset_grp = personalize.list_dataset_groups()
    datasetGroupArn = dataset_grp["datasetGroups"][0]["datasetGroupArn"]
    return personalize, response['recipes'], datasetGroupArn


def user_personalization_recipe(personalize, response_recipies, datasetGroupArn):
    # Creating solution
    create_solution_response = personalize.create_solution(name='user_personalize_solution',
                                                           recipeArn=response_recipies[6]['recipeArn'],
                                                           datasetGroupArn=datasetGroupArn)
    solution_arn = create_solution_response['solutionArn']
    create_solution_version_response = personalize.create_solution_version(solutionArn=solution_arn,
                                                                           trainingMode='FULL')

    new_solution_version_arn = create_solution_version_response['solutionVersionArn']
    time.sleep(60 * 100)
    create_campaign_response = personalize.create_campaign(
        name='personalize_campaign',
        solutionVersionArn=new_solution_version_arn,
        minProvisionedTPS=1
    )

    campaign_arn = create_campaign_response['campaignArn']
    print('campaign_arn:', campaign_arn)


if __name__ == '__main__':
    personalize, response_recipies, datasetGroupArn = verifying_env()
    user_personalization_recipe(personalize, response_recipies, datasetGroupArn)