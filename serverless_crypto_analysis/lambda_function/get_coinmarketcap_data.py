import datetime
import json
import logging
import os
import time

import boto3
import requests
from enum import Enum
from serverless_crypto_analysis.utils.enums import S3Enums


def lambda_handler(event, context):
    bucket = os.environ[S3Enums.DESTINATION_BUCKET.value]
    print(bucket)
    key = os.environ[S3Enums.DESTINATION_KEY.value]
    print(bucket)
    now = datetime.datetime.now()
    uuid = int(time.time())
    logging.info("uuid {}".format(uuid))
    response = query_coinmarketcap()

    full_dest_key = build_s3_key(key, now, uuid)
    boto3.client("s3").put_object(
        Body=json.dumps(response), Bucket=bucket, Key=full_dest_key
    )
    return bucket, full_dest_key


def query_coinmarketcap():
    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
    querystring = {"limit": "200"}
    headers = {
        "cache-control": "no-cache",
        "postman-token": "559e252d-ca13-c52c-7667-107f809d9520",
        "X-CMC_PRO_API_KEY": "fc7667dd-7bd0-4b93-8459-fa8299b0c7e9",
    }
    response = requests.request("GET", url, headers=headers, params=querystring)
    parsed_response = json.loads(response.text)
    return parsed_response


def build_s3_key(key, now, uuid):
    full_key = os.path.join(
        key,
        "year={}".format(now.year),
        "month={}".format(now.month),
        "day={}".format(now.day),
        "{}.json".format(uuid),
    )
    logging.info("key to dump {}".format(full_key))
    return full_key
