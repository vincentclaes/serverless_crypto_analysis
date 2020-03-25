import json
import os

import awswrangler as wr
import pandas as pd

from serverless_crypto_analysis.utils.lambda_utils import (
    get_bucket_from_event, get_key_from_event)
from serverless_crypto_analysis.utils.s3_utils import (build_s3_url,
                                                       get_object_from_s3)


def lambda_handler(event, context):
    # read data
    source_bucket = get_bucket_from_event(event)
    source_key = get_key_from_event(event)
    s3_object = get_object_from_s3(source_bucket, source_key)
    json_object = json.loads(s3_object)

    # do transformation
    df = pd.DataFrame(json_object.get("data"), columns=["slug", "cmc_rank"])
    df["id"] = df["slug"]
    df["rank"] = df["cmc_rank"]
    del df["slug"]
    del df["cmc_rank"]
    df["uuid"] = get_uuid_from_key(source_key)
    df["partition"] = 1
    df = df.astype({"id": str, "rank": int, "uuid": int})
    # write data
    write_to_parquet(df)
    return df

def write_to_parquet(df):
    destination_bucket = os.environ["DESTINATION_BUCKET"]
    destination_key = os.environ["DESTINATION_KEY"]
    s3_url = build_s3_url(destination_bucket, destination_key)

    wr.pandas.to_parquet(
        dataframe=df,
        path=s3_url,
        partition_cols=["partition"],
        mode="append"
    )


def get_uuid_from_key(key):
    return os.path.split(key)[1].split(".json")[0]
