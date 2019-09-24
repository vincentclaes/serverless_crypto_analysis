import json
import time
import datetime
import requests
import os
import boto3

def handler(event, context):
    response = query_coinmarketcap()
    bucket = os.environ["BUCKET"]
    key = os.environ["KEY"]
    s3_full_key = build_s3_full_key()
    s3 = boto3.client('s3').put_object(Bucket=bucket, Key=key, Body=json.dumps(response))


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


def build_s3_full_key(key):
    now = datetime.datetime.now()
    uuid = int(time.time())
    return 's3://serverless-crypto-analysis/raw/coinmarketcap_data/date={}/hour={}/minute={}/uuid_string={}/{}.parquet'.format(
        now.strftime("%Y"), now.strftime("%m"), now.strftime("%d"), uuid, now.isoformat())
