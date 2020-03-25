import json
import os
import unittest

from moto import mock_s3

from serverless_crypto_analysis.lambda_function import get_coinmarketcap_data
from serverless_crypto_analysis.utils.enums import S3Enums
from serverless_crypto_analysis.utils.s3_utils import (create_bucket,
                                                       get_object_from_s3)

DIR_PATH = os.path.dirname(os.path.realpath(__file__))


class TestGetCoinmarketCapData(unittest.TestCase):
    @mock_s3
    def test_handler(self):
        os.environ[S3Enums.DESTINATION_BUCKET.name] = S3Enums.DESTINATION_BUCKET.value
        os.environ[S3Enums.DESTINATION_KEY.name] = S3Enums.DESTINATION_KEY.value
        create_bucket(S3Enums.DESTINATION_BUCKET.name)

        bucket, key = get_coinmarketcap_data.lambda_handler(event=None, context=None)
        result = get_object_from_s3(bucket, key)
        result = json.loads(result)
        self.assertTrue(result.get("status"))
        self.assertEqual(len(result.get("data")), 200)
