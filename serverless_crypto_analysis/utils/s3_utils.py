import boto3


def create_bucket(bucket_name):
    boto3.client("s3").create_bucket(Bucket=bucket_name)


def get_objects_in_bucket(bucket):
    # return boto3.client("s3").list_objects_v2(Bucket=bucket)["Contents"]
    return [s3_object for s3_object in boto3.resource('s3').Bucket(bucket).objects.all()]


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
