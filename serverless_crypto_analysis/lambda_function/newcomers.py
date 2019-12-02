def lambda_handler(event, context):
        # SQL Query to execute

    query = ("""SELECT max(uuid) FROM coinmarketcap_coinmarketcap_data""")
    print("Executing query: {}".format(query))
    result = run_query(query, "coinmarketcap", "s3://aws-athena-query-results-077590795309-eu-central-1")
    print("result: {}".format(result))

#!/usr/bin/env python3
#
# Query AWS Athena using SQL
# Copyright (c) Alexey Baikov <sysboss[at]mail.ru>
#
# This snippet is a basic example to query Athen and load the results
# to a variable.
#
# Requirements:
# > pip3 install boto3 botocore retrying

import csv
import os

import boto3
import botocore

from retrying import retry

# configuration
# s3_bucket = 'aws-athena-query-results-077590795309-eu-central-1'       # S3 Bucket name
# s3_ouput  = 's3://'+ s3_bucket   # S3 Bucket to store results
# database  = 'coinmarketcap'  # The database to which the query belongs

# init clients
athena = boto3.client('athena')
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

@retry(stop_max_attempt_number = 10,
    wait_exponential_multiplier = 300,
    wait_exponential_max = 1 * 60 * 1000)
def poll_status(_id):
    result = athena.get_query_execution( QueryExecutionId = _id )
    state  = result['QueryExecution']['Status']['State']

    if state == 'SUCCEEDED':
        return result
    elif state == 'FAILED':
        return result
    else:
        raise Exception

def run_query(query, database, s3_output):
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': s3_output,
    })

    QueryExecutionId = response['QueryExecutionId']
    result = poll_status(QueryExecutionId)

    if result['QueryExecution']['Status']['State'] == 'SUCCEEDED':
        print("Query SUCCEEDED: {}".format(QueryExecutionId))

        s3_key = QueryExecutionId + '.csv'
        local_filename = QueryExecutionId + '.csv'

        # download result file
        try:
            response = s3_client.get_object(Bucket=s3_output, Key=s3_key)
            lines = response['Body'].read().decode('utf-8').split()

        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise

        # read file to array
        rows = []
        reader = csv.DictReader(lines)
        for row in reader:
            rows.append(row)

        # delete result file
        if os.path.isfile(local_filename):
            os.remove(local_filename)

        return rows

# if __name__ == '__main__':


#     print("Results:")
#     print(result)
