import boto3
import os
import pyathena
import pandas as pd
def handler(event, context):
    os.environ['AWS_PROFILE']='serverless'
    # client = boto3.client('athena')

    # queryStart = client.start_query_execution(
    #     QueryString = 'SELECT * FROM coinmarketcap_data',
    #     QueryExecutionContext = {
    #         'Database': 'coinmarketcap'
    #     },
    #     ResultConfiguration={
    #         'OutputLocation': 's3://aws-athena-query-results-077590795309-eu-central-1'
    #     }   
    # )

    # result = client.get_query_results(
    # QueryExecutionId=queryStart['QueryExecutionId']
    #  )

    from pyathena import connect

    cursor = connect(profile_name='serverless', schema_name='coinmarketcap', s3_staging_dir='s3://aws-athena-query-results-077590795309-eu-central-1').cursor()
    conn = connect(profile_name='serverless', schema_name='coinmarketcap', s3_staging_dir='s3://aws-athena-query-results-077590795309-eu-central-1')
    # cursor.execute("SELECT * FROM coinmarketcap_data")
    # print(cursor.description)
    # print(cursor.fetchall())
    df = pd.read_sql("SELECT * FROM coinmarketcap_data", conn)
    df
    # get latest uuid
    # SELECT max(uuid) FROM "coinmarketcap"."coinmarketcap_data";
    
    # get newcomer for uuid
    # "SELECT * FROM crypto_data where uuid=={}".format(uuid)
    # get newcomers below this uuid
    # "SELECT DISTINCT id FROM crypto_data where uuid<{} and rank <= {}".format(uuid, rank)
    # is there an id in the frist query that is not there in the second

    # write a key to s3?
    # if there is at least one you can trigger another lambda
    pass

handler(None, None)