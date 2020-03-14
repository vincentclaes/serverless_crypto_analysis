import urllib

from loguru import logger


def get_key_from_event(event):
    key = event["Records"][0]["s3"]["object"]["key"]
    logger.info("key: {}".format(key))
    key_ = urllib.parse.unquote_plus(key, encoding="utf-8")
    logger.info("key_: {}".format(key_))
    return key_


def get_bucket_from_event(event):
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    logger.info("bucket: {}".format(bucket))
    return bucket