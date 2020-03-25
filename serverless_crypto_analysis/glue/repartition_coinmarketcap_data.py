import argparse
import datetime
import io
import logging

import awswrangler as wr
import boto3
import pandas as pd


def pd_read_s3_parquet(bucket, key, s3_client=None, **args):
    if s3_client is None:
        s3_client = boto3.client("s3")
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()), **args)


def pd_read_s3_multiple_parquets(
    bucket, filepath, s3=None, s3_client=None, verbose=False, **args
):
    if not filepath.endswith("/"):
        filepath = filepath + "/"  # Add '/' to the end
    if s3_client is None:
        s3_client = boto3.client("s3")
    if s3 is None:
        s3 = boto3.resource("s3")
    s3_keys = [
        item.key
        for item in s3.Bucket(bucket).objects.filter(Prefix=filepath)
        if item.key.endswith(".parquet")
    ]
    if not s3_keys:
        logging.info("No parquet found in", bucket, filepath)
    elif verbose:
        logging.info("Load parquets:")
        for p in s3_keys:
            logging.info(p)
    dfs = [
        pd_read_s3_parquet(bucket=bucket, key=key, s3_client=s3_client, **args)
        for key in s3_keys
    ]
    return pd.concat(dfs, ignore_index=True)


def convert_types(df):
    logging.info("columns {}".format(df.columns))
    logging.info("dtypes before conversion {}".format(df.dtypes))
    column_type_mapping = {
        "id": str,
        "name": str,
        "symbol": str,
        "rank": pd.to_numeric,
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
        logging.info(column, type_func)
        if not isinstance(type_func, type):
            df_updated[column] = type_func(df[column])
        else:
            df_updated[column] = df[column]
    df_updated = df_updated[~df_updated["rank"].isnull()]
    df_updated["rank"] = df_updated["rank"].astype("int64")
    logging.info("dtypes after conversion {}".format(df.dtypes))
    return df_updated


def repartition(bucket, key, backup_bucket, backup_key):
    df = pd_read_s3_multiple_parquets(bucket, key)
    df = convert_types(df)
    wr.pandas.to_parquet(
        dataframe=df,
        path="s3://{}/{}/{}".format(
            backup_bucket, backup_key, datetime.datetime.now().isoformat()
        ),
        preserve_index=False,
    )
    wr.pandas.to_parquet(
        dataframe=df,
        mode="overwrite",
        path="s3://{}/{}".format(bucket, key),
        preserve_index=False,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", type=str, required=True)
    parser.add_argument("--key", type=str, required=True)
    parser.add_argument("--backup_bucket", type=str, required=True)
    parser.add_argument("--backup_key", type=str, required=True)

    args, unknown = parser.parse_known_args()
    logging.warning("unknown args found : {}".format(unknown))
    repartition(args.bucket, args.key, args.backup_bucket, args.backup_key)
