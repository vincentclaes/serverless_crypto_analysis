import os

import boto3


def create_bucket(bucket_name):
    boto3.client("s3").create_bucket(Bucket=bucket_name)


def get_objects_in_bucket(bucket):
    return boto3.client("s3").list_objects_v2(Bucket=bucket)["Contents"]


def get_object_from_s3(bucket, key):
    s3_object = (
        boto3.client("s3")
        .get_object(Bucket=bucket, Key=key)["Body"]
        .read()
        .decode("utf-8")
    )
    return s3_object


def put_s3_object(object_, destination_bucket, key):
    boto3.client("s3").put_object(Body=object_, Bucket=destination_bucket, Key=key)


def build_s3_url(bucket, key):
    full_s3_path = os.path.join(bucket, key)
    return "s3://{}".format(full_s3_path)
