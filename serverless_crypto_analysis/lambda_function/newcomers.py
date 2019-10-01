# import boto3
import os
import pyathena
import pandas as pd

def handler(event, context):
    os.environ['AWS_PROFILE']='serverless'
    rank = 100
    from pyathena import connect

    conn = get_connection()
    max_uuid = get_max_uuid(conn)
    df_last = get_last_df(conn, uuid)
    df_tail = get_tail_df(conn, uuid, rank)
    newcomers = get_newcomers(df_last, df_tail)
    return newcomers
    # cursor.fetchall()
    # print(cursor.description)
    # print(cursor.fetchall())
    # df = pd.read_sql("SELECT * FROM coinmarketcap_data", conn)
    # get latest uuid
    # SELECT max(uuid) FROM "coinmarketcap"."coinmarketcap_data";
    
    # get newcomer for uuid
    # "SELECT * FROM crypto_data where uuid=={}".format(uuid)
    # get newcomers below this uuid
    # "SELECT DISTINCT id FROM crypto_data where uuid<{} and rank <= {}".format(uuid, rank)
    # is there an id in the frist query that is not there in the second

    # write a key to s3?
    # if there is at least one you can trigger another lambda

def get_connection():
    return connect(profile_name='serverless', schema_name='coinmarketcap', s3_staging_dir='s3://aws-athena-query-results-077590795309-eu-central-1')

def get_max_uuid(conn):
    return conn.cursor().execute("SELECT max(uuid) FROM coinmarketcap_data limit 1").fetchone()[0]

def get_last_df(conn, uuid):
    return pd.read_sql("SELECT * FROM crypto_data where uuid=={}".format(uuid))

def get_tail_df(conn, uuid, rank):
    return pd.read_sql("SELECT DISTINCT id FROM crypto_data where uuid<{} and rank <= {}".format(uuid, rank))

def get_newcomers(df_last, df_tail):
    newcomers = df_last[~df_last["id"].isin(df_tail["id"])]
    return newcomers