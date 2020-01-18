import unittest
from serverless_crypto_analysis.glue import get_coinmarketcap_data
from serverless_crypto_analysis.utils.s3_utils import create_bucket
from moto import mock_s3


class MyTestCase(unittest.TestCase):
    @mock_s3
    def test_get_data_successfully(self):
        bucket = "bucket"
        create_bucket(bucket)
        s3_keys = get_coinmarketcap_data.get_coinmarketcap_data(bucket, "key")
        # tests ran without error
        self.assertTrue(s3_keys)


if __name__ == "__main__":
    unittest.main()
