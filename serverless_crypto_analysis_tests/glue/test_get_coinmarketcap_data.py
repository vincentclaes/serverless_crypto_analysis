import unittest
from serverless_crypto_analysis.glue import get_coinmarketcap_data
from serverless_crypto_analysis.utils.s3_utils import create_bucket
from moto import mock_s3
from serverless_crypto_analysis.utils.s3_utils import get_objects_in_bucket
from serverless_crypto_analysis.utils.s3_utils import get_object_from_s3

class MyTestCase(unittest.TestCase):

    @mock_s3
    def test_get_data_successfully(self):
        bucket = "bucket"
        create_bucket(bucket)
        get_coinmarketcap_data.get_coinmarketcap_data(bucket, "key")
        objects = get_objects_in_bucket(bucket)
        get_object_from_s3(bucket, "")
        self.assertEqual(True, False)


if __name__ == '__main__':
    unittest.main()
