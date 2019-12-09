import json
import time
import datetime

import pandas as pd
import requests
import logging
import argparse
import os


def get_coinmarketcap_data(bucket, key):
    now = datetime.datetime.now()
    uuid = int(time.time())
    logging.info("uuid {}".format(uuid))
    response = query_coinmarketcap()

    df = convert_dict_to_df(response, now, uuid)
    s3_path = build_s3_full_path(now, bucket, key)
    df.to_parquet(s3_path, compression="gzip")
    return df


def convert_dict_to_df(response, now, uuid):
    df = pd.DataFrame(response)
    df["date"] = str(now.date())
    df["hour"] = now.hour
    df["minute"] = now.minute
    df["uuid"] = uuid
    df = convert_types(df)
    return df


def query_coinmarketcap():
    url = "https://api.coinmarketcap.com/v1/ticker/"
    querystring = {"limit": "200"}
    headers = {
        "cache-control": "no-cache",
        "postman-token": "559e252d-ca13-c52c-7667-107f809d9520",
    }
    response = requests.request("GET", url, headers=headers, params=querystring)
    parsed_response = json.loads(response.text)
    return parsed_response


def build_s3_full_path(now, bucket, key):
    full_key = os.path.join(bucket, key, now.isoformat() + ".parquet")
    logging.info("key to dump {}".format(full_key))
    return "s3://{}".format(full_key)


def convert_types(df):
    print(df.columns)
    types = [
        pd.to_numeric,
        pd.to_numeric,
        str,
        pd.to_numeric,
        pd.to_numeric,
        pd.to_numeric,
        str,
        pd.to_numeric,
        pd.to_numeric,
        pd.to_numeric,
        pd.to_numeric,
        pd.to_numeric,
        pd.to_numeric,
        str,
        pd.to_numeric,
        pd.to_datetime,
        pd.to_numeric,
        pd.to_numeric,
        pd.to_numeric,
    ]
    df_updated = pd.DataFrame()
    column_type_mapping = zip(types, df.columns)
    for type_func, column in column_type_mapping:
        if not isinstance(type_func, type):
            print(column)
            df_updated[column] = type_func(df[column])
        else:
            df_updated[column] = df[column]
    return df_updated


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", type=str, required=True)
    parser.add_argument("--key", type=str, required=True)

    args, unknown = parser.parse_known_args()
    logging.warning("unknown args found : {}".format(unknown))
    get_coinmarketcap_data(args.bucket, args.key)
