import json
import time
import datetime

import pandas as pd
import requests


def get_coinmarketcap_data():
    now = datetime.datetime.now()
    uuid = int(time.time())
    response = query_coinmarketcap()

    df = convert_dict_to_df(response, now, uuid)
    s3_path = build_s3_full_path(now, uuid)
    df.to_parquet(s3_path, compression='gzip')
    return df


def convert_dict_to_df(response, now, uuid):
    df = pd.DataFrame(response)
    df['date'] = str(now.date())
    df['hour'] = now.hour
    df['minute'] = now.minute
    df['uuid'] = uuid
    df = convert_types(df)
    return df


def query_coinmarketcap():
    url = "https://api.coinmarketcap.com/v1/ticker/"
    querystring = {"limit": "200"}
    headers = {
        'cache-control': "no-cache",
        'postman-token': "559e252d-ca13-c52c-7667-107f809d9520"
    }
    response = requests.request("GET", url, headers=headers, params=querystring)
    parsed_response = json.loads(response.text)
    return parsed_response


def build_s3_full_path(now, uuid):
    # return 's3://serverless-crypto-analysis/raw/coinmarketcap_data/date={}/hour={}/minute={}/uuid_string={}/{}.parquet'.format(
    #     now.strftime("%Y"), now.strftime("%m"), now.strftime("%d"), uuid, now.isoformat())
    return 's3://serverless-crypto-analysis/raw/coinmarketcap_data/{}/{}/{}/{}/{}.parquet'.format(
        now.strftime("%Y"), now.strftime("%m"), now.strftime("%d"), uuid, now.isoformat())


def convert_types(df):
    print(df.columns)
    types = [pd.to_numeric, pd.to_numeric, str, pd.to_numeric, pd.to_numeric, pd.to_numeric, str, \
             pd.to_numeric, pd.to_numeric, pd.to_numeric, pd.to_numeric, pd.to_numeric, pd.to_numeric, \
             str, pd.to_numeric, pd.to_datetime, pd.to_numeric, pd.to_numeric, pd.to_numeric]
    df_updated = pd.DataFrame()
    column_type_mapping = zip(types, df.columns)
    for type_func, column in column_type_mapping:
        if not isinstance(type_func, type):
            print(column)
            df_updated[column] = type_func(df[column])
        else:
            df_updated[column] = df[column]
    return df_updated


if __name__ == '__main__':
    get_coinmarketcap_data()
