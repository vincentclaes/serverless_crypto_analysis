import datetime
import json
import os
import time

import awswrangler as wr
import boto3

# init clients
lambda_client = boto3.client("lambda")


def lambda_handler(event, context):
    df = get_distinct_uuid()
    df["date"] = df["uuid"].apply(lambda x: datetime.datetime.fromtimestamp(x).date())
    df["time"] = df["uuid"].apply(lambda x: datetime.datetime.fromtimestamp(x).time())
    lookback_period = os.environ["LOOKBACK_PERIOD"]
    year = os.environ.get("YEAR")
    month = os.environ.get("MONTH")
    day = os.environ.get("DAY")
    if year:
        year = int(year)
        month = int(month)
        day = int(day)
        df = df[df["date"] > datetime.date(year,month,day)]
    df = df.groupby("date")["uuid"].agg(time= max)
    df["uuid"] = df["time"]
    for uuid in df["uuid"].sort_values():
        print("invoking for date {}".format(uuid))
        trigger_lambda(uuid, lookback_period)


def trigger_lambda(uuid, lookback_period):
    """
    trigger lambda asynchronuous by using "Event" as invocationtype.
    :param uuid:
    :return:
    """
    function_name = os.environ["FUNCTION_NAME"]
    response = lambda_client.invoke(
        FunctionName=function_name,
        InvocationType="Event",
        Payload=json.dumps({"uuid": uuid, "lookback_period": lookback_period})
    )
    time.sleep(0.5)
    print(response)


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
