import json
import time
import datetime

import pandas as pd
import requests
import logging
import argparse
import os
import awswrangler as wr


def get_coinmarketcap_data(bucket, key):
    now = datetime.datetime.now()
    uuid = int(time.time())
    logging.info("uuid {}".format(uuid))
    response = query_coinmarketcap()

    df = convert_dict_to_df(response, now, uuid)
    s3_path = build_s3_full_path(now, bucket, key)
    s3_keys = wr.pandas.to_parquet(dataframe=df, path=s3_path, preserve_index=False)
    return s3_keys


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
    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
    querystring = {"limit": "200"}
    headers = {
        "cache-control": "no-cache",
        "postman-token": "559e252d-ca13-c52c-7667-107f809d9520",
        "X-CMC_PRO_API_KEY": "fc7667dd-7bd0-4b93-8459-fa8299b0c7e9",
    }
    response = requests.request("GET", url, headers=headers, params=querystring)
    parsed_response = json.loads(response.text)
    return parsed_response.get("data")


def build_s3_full_path(now, bucket, key):
    full_key = os.path.join(bucket, key)
    logging.info("key to dump {}".format(full_key))
    return "s3://{}".format(full_key)


def convert_types(df):
    print(df.dtypes)
    column_type_mapping = {
        "id": str,
        "name": str,
        "symbol": str,
        "cmc_rank": pd.to_numeric,
        "price_usd": pd.to_numeric,
        "price_btc": pd.to_numeric,
        "24h_volume_usd": pd.to_numeric,
        "market_cap_usd": pd.to_numeric,
        "available_supply": pd.to_numeric,
        "total_supply": pd.to_numeric,
        "max_supply": pd.to_numeric,
        "percent_change_1h": pd.to_numeric,
        "percent_change_24h": pd.to_numeric,
        "percent_change_7d": pd.to_numeric,
        "last_updated": pd.to_numeric,
        "date": pd.to_datetime,
        "hour": pd.to_numeric,
        "minute": pd.to_numeric,
        "uuid": pd.to_numeric,
    }
    df_updated = pd.DataFrame()
    for column, type_func in column_type_mapping.items():
        print(column, type_func)
        if not isinstance(type_func, type):
            df_updated[column] = type_func(df[column])
        else:
            df_updated[column] = df[column]
    df_updated = df_updated[~df_updated["rank"].isnull()]
    df_updated["rank"] = df_updated["rank"].astype("float64")
    print(df.dtypes)
    return df_updated


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", type=str, required=True)
    parser.add_argument("--key", type=str, required=True)

    args, unknown = parser.parse_known_args()
    logging.warning("unknown args found : {}".format(unknown))
    get_coinmarketcap_data(args.bucket, args.key)
