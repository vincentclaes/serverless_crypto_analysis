import argparse
import json
import os
import sys
import logging

import boto3
import pyarrow.parquet as pq
import s3fs


def check_for_newcomers(bucket, source_key, rank):
    print("args {}".format(sys.argv))
    df = read_data_source(bucket, source_key)
    max_uuid = get_max_of_column(df, "uuid")
    df_last = get_last_df(df, max_uuid)
    df_tail = get_tail_df(df, max_uuid, rank)
    newcomers = get_newcomers(df_last, df_tail)
    return newcomers


def read_data_source(bucket, key):
    s3 = s3fs.S3FileSystem()
    pandas_dataframe = (
        pq.ParquetDataset("s3://{}/{}".format(bucket, key), filesystem=s3)
        .read_pandas()
        .to_pandas()
    )
    return pandas_dataframe


def get_max_of_column(df, column):
    max_ = df[column].max()
    logging.info(f"found max value {max_} for column {column}")
    return max_


def get_last_df(df, max_uuid):
    return df[df["uuid"] == max_uuid]


def get_tail_df(df, max_uuid, rank):
    return df[(df["uuid"] < max_uuid) & (df["rank"] <= rank)]


def get_newcomers(df_last, df_tail):
    newcomers = df_last[~df_last["id"].isin(df_tail["id"])]
    logging.info(f"found newcomers {newcomers}")
    return newcomers


def dump_newcomers_as_json(newcomers, bucket, dest_key_json):
    list_of_newcomers = newcomers.to_dict("records")
    for newcomer in list_of_newcomers:
        coin_id = newcomer["id"]
        boto3.client("s3").put_object(
            Body=json.dumps(newcomer, indent=4, sort_keys=True, default=str),
            Bucket=bucket,
            Key=os.path.join(dest_key_json, coin_id + ".json"),
        )
    return list_of_newcomers


def dump_newcomers_as_parquet(newcomers, bucket, dest_key_parquet):
    if not newcomers.empty:
        uuid = newcomers.uuid.values[0]
        key_with_file_name = os.path.join(
            bucket, dest_key_parquet, "{}.parquet".format(uuid)
        )
        s3_url = f"s3://{key_with_file_name}"
        dump_df_to_s3_as_parquet(s3_url, compression="gzip")


def dump_df_to_s3_as_parquet(df, s3_url):
    df.to_parquet(s3_url, compression="gzip")


def main():
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", type=str)
    parser.add_argument("--source_key", type=str)
    parser.add_argument("--dest_key_json", type=str)
    parser.add_argument("--dest_key_parquet", type=str)
    parser.add_argument("--rank", type=int)
    args, unknown = parser.parse_known_args()
    logging.warning(f"unknown args found : {unknown}")
    newcomers = check_for_newcomers(args.bucket, args.source_key, args.rank)
    newcomers_json = dump_newcomers_as_json(newcomers, args.bucket, args.dest_key_json)
    dump_newcomers_as_parquet(newcomers, args.bucket, args.dest_key_parquet)

    return newcomers, newcomers_json


if __name__ == "__main__":
    main()
