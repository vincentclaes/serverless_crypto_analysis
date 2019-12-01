import os
import sys
import unittest
import boto3
import pandas as pd

from mock import patch
from serverless_crypto_analysis.glue import newcomers
from moto import mock_s3

DIR_PATH = os.path.dirname(os.path.realpath(__file__))


class TestNewcomers(unittest.TestCase):

    def setUp(self):
        self.df = pd.read_csv(os.path.join(DIR_PATH, 'resources', 'test_newcomers'), index_col=0)

    def create_bucket(self, bucket_name):
        boto3.client("s3").create_bucket(Bucket=bucket_name)

    def get_objects_in_bucket(self, bucket):
        return boto3.client("s3").list_objects_v2(
            Bucket=bucket)['Contents']

    @mock_s3
    @patch('serverless_crypto_analysis.glue.newcomers.dump_df_to_s3_as_parquet')
    @patch('serverless_crypto_analysis.glue.newcomers.read_data_source')
    def test_get_newcomers(self, m_read_data, m_write_parquet):
        # arrange
        # add new coins
        self.df['id'][0] = "my_new_coin_1"
        self.df['id'][1] = "my_new_coin_2"

        m_read_data.return_value = self.df

        bucket = 'bucket'
        self.create_bucket(bucket)
        args_list = ['--bucket', bucket, '--source_key', 'raw/coinmarketcap', '--dest_key_json',
                     'stg/newcomers/json', '--dest_key_parquet', 'stg/newcomers/parquet', '--rank', '100']
        sys.argv.extend(args_list)

        result = newcomers.main()

        objects = self.get_objects_in_bucket(bucket)
        self.assertEqual(len(objects), 2)
        result_df = result[0]
        self.assertListEqual(result_df.id.values.tolist(), ['my_new_coin_1', 'my_new_coin_2'])
