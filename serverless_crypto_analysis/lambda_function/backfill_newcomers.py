import csv
import os
import json
import boto3
import botocore
import datetime
import awswrangler as wr
from retrying import retry

# init clients
lambda_client = boto3.client("lambda")


def lambda_handler(event, context):
    df = get_distinct_uuid()
    df["date"] = df["uuid"].apply(lambda x: datetime.datetime.fromtimestamp(x).date())
    df["time"] = df["uuid"].apply(lambda x: datetime.datetime.fromtimestamp(x).time())
    df = df.groupby("date")["uuid"].agg({"time": max})
    df["uuid"] = df["time"]
    for uuid in df["uuid"].sort_values():
        print("invoking for date {}".format(uuid))
        function_name = os.environ["FUNCTION_NAME"]
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType="RequestResponse",
            Payload={"uuid" : uuid}
        )
        print(response)

        lambda_client.invoke()

def get_distinct_uuid():
    athena_db = os.environ["ATHENA_DB"]
    print(athena_db)
    athena_table = os.environ["ATHENA_TABLE"]
    # athena_table = "prd_coinmarketcap_data"
    print(athena_table)
    df = wr.pandas.read_sql_athena(
        sql="select distinct(uuid) from {}".format(athena_table),
        database=athena_db
    )
    return df
