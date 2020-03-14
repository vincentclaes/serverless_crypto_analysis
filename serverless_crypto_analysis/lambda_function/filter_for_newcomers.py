from serverless_crypto_analysis.utils.lambda_utils import get_bucket_from_event
from serverless_crypto_analysis.utils.lambda_utils import get_key_from_event
from serverless_crypto_analysis.utils.s3_utils import build_s3_url

def lambda_handler(event, context):
    bucket = get_bucket_from_event(event)
    key = get_key_from_event(event)
    s3_url = build_s3_url(bucket, key)
