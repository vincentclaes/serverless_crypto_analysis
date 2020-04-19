import json
import os

import awswrangler as wr
import pandas as pd
from loguru import logger
from serverless_crypto_analysis.utils.lambda_utils import (
    get_bucket_from_event, get_key_from_event)
from serverless_crypto_analysis.utils.s3_utils import (build_s3_url,
                                                       get_object_from_s3)
from serverless_crypto_analysis.utils import s3_utils


def lambda_handler(event, context):
    # read data
    print(event)
    source_bucket = get_bucket_from_event(event)
    source_key = get_key_from_event(event)
    s3_object = get_object_from_s3(source_bucket, source_key)
    json_object = json.loads(s3_object)
    # do transformation
    logger.info("read df")
    df = pd.DataFrame(json_object["data"], columns=["slug", "cmc_rank"])
    logger.info("transform df")
    df["id"] = df["slug"]
    df["rank"] = df["cmc_rank"]
    del df["slug"]
    del df["cmc_rank"]
    df["uuid"] = get_uuid_from_key(source_key)
    df = df.astype({"id": str, "rank": int, "uuid": int})
    # write data
    logger.info("write df")
    write_to_parquet(df)


def write_to_parquet(df):
    destination_bucket = os.environ["DESTINATION_BUCKET"]
    destination_key = os.environ["DESTINATION_KEY"]

    s3_url = build_s3_url(destination_bucket, destination_key)
    if wr.s3.list_objects(s3_url):
        logger.debug("read the existing destination dataframe {}".format(s3_url))
        df_destination = wr.s3.read_parquet(s3_url)
        df_append = df.append(df_destination, ignore_index=True)
        logger.debug("use the latest columns for your destination {}".format(df.columns.tolist()))
        df = df_append[df.columns.tolist()]
        logger.debug("write to {}".format(s3_url))
        write_to_s3(df, s3_url, mode="overwrite")
        return
    write_to_s3(df, s3_url, mode="append")


def write_to_s3(df, path, mode):
    wr.s3.to_parquet(
        df=df,
        path=path,
        mode=mode,
        dataset=True
    )


def get_uuid_from_key(key):
    return os.path.split(key)[1].split(".json")[0]
