import os

import pandas as pd
import pyarrow.parquet as pq
import s3fs
import sys

def check_for_newcomers():
    print('args {}'.format(sys.argv))
    bucket = 'serverless-crypto-analysis'
    key = 'raw'
    rank= 100
    df = read_data_source(bucket, key)
    max_uuid = get_max_of_column(df, 'uuid')
    df_last = get_last_df(df, max_uuid)
    df_tail = get_tail_df(df, max_uuid, rank)
    newcomers = get_newcomers(df_last, df_tail)
    return newcomers

def read_data_source(bucket, key):
    s3 = s3fs.S3FileSystem()
    pandas_dataframe = pq.ParquetDataset('s3://{}/{}'.format(bucket, key), filesystem=s3).read_pandas().to_pandas()
    return pandas_dataframe


def get_max_of_column(df, column):
    return df[column].max()


def get_last_df(df, max_uuid):
    return df[df['uuid'] == max_uuid]

def get_tail_df(df, max_uuid, rank):
    return df[df['uuid'] < max_uuid and df['rank'] <= rank]
    # return pd.read_sql("SELECT DISTINCT id FROM crypto_data where uuid<{} and rank <= {}".format(uuid, rank))


def get_newcomers(df_last, df_tail):
    newcomers = df_last[~df_last["id"].isin(df_tail["id"])]
    return newcomers
