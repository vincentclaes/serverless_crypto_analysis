#!/Users/vincent/anaconda3/bin/python
import json
import time
from datetime import datetime

import pandas as pd
import requests


def get_data(event=None):
    url = "https://api.coinmarketcap.com/v1/ticker/"

    querystring = {"limit": "200"}

    headers = {
        'cache-control': "no-cache",
        'postman-token': "559e252d-ca13-c52c-7667-107f809d9520"
    }

    response = requests.request("GET", url, headers=headers, params=querystring)

    parsed_response = json.loads(response.text)

    df = pd.DataFrame(parsed_response)
    now = datetime.now()
    df['date'] = str(now.date())
    df['hour'] = now.hour
    df['minute'] = now.minute
    df['uuid'] = int(time.time())

    df = convert_types(df)
    s3_path = build_s3_full_path(now)
    df.to_parquet(s3_path)


def build_s3_full_path(now):
    return 's3://serverless-crypto-analysis/coinmarketcap_data/{}/{}.parquet'.format(now.date(), now)

def convert_types(df):
    types = [pd.to_numeric, pd.to_numeric, str, pd.to_numeric, pd.to_numeric, pd.to_numeric, str, \
             pd.to_numeric, pd.to_numeric, pd.to_numeric, pd.to_numeric, pd.to_numeric, pd.to_numeric, \
             str, pd.to_numeric, pd.to_datetime, pd.to_numeric, pd.to_numeric, pd.to_numeric]
    df_updated = pd.DataFrame()
    column_type_mapping = zip(types, df.columns)
    for type_func, column in column_type_mapping:
        if not isinstance(type_func, type):
            df_updated[column] = type_func(df[column])
        else:
            df_updated[column] = df[column]
    return df_updated


def prep_date_for_s3_folder_structure(date_):
    return str(date_.date()).replace('-', '/')

def remove_space_in_date(date_):
    return str(date_).replace(' ', '-')

def handler(event, context):
    get_data(event)

