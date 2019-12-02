import csv
import os

import boto3
import botocore

from retrying import retry

# init clients
athena = boto3.client('athena')
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')


def lambda_handler(event, context):
    query = ("""SELECT max(uuid) FROM coinmarketcap_coinmarketcap_data""")
    print("Executing query: {}".format(query))
    result = run_query(query, "coinmarketcap", "s3://aws-athena-query-results-077590795309-eu-central-1",
                       "aws-athena-query-results-077590795309-eu-central-1")
    print("result: {}".format(result))


@retry(stop_max_attempt_number=10,
       wait_exponential_multiplier=300,
       wait_exponential_max=1 * 60 * 1000)
def poll_status(_id):
    result = athena.get_query_execution(QueryExecutionId=_id)
    state = result['QueryExecution']['Status']['State']

    if state == 'SUCCEEDED':
        return result
    elif state == 'FAILED':
        return result
    else:
        raise Exception


def run_query(query, database, s3_output, output_bucket):
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
            response = s3_client.get_object(Bucket=output_bucket, Key=s3_key)
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
