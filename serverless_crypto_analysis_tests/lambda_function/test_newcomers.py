import os
import unittest
import boto3
from collections import OrderedDict

from mock import patch

from serverless_crypto_analysis.lambda_function import newcomers

DIR_PATH = os.path.dirname(os.path.realpath(__file__))


class TestNewcomers(unittest.TestCase):
    def create_bucket(self, bucket_name):
        boto3.client("s3").create_bucket(Bucket=bucket_name)

    def get_objects_in_bucket(self, bucket):
        return boto3.client("s3").list_objects_v2(
            Bucket=bucket)['Contents']

    @patch("serverless_crypto_analysis.lambda_function.newcomers.run_query")
    def test_max_uuid(self, m_query):
        m_query.return_value = [OrderedDict([('_col0', '1575396043')])]
        result = newcomers.get_max_uuid()
        self.assertEqual(result, '1575396043')

    @patch("serverless_crypto_analysis.lambda_function.newcomers.run_query")
    def test_latest_results(self, m_query):
        m_query.return_value = [OrderedDict([('id', 'bitcoin')]), OrderedDict([('id', 'ethereum')]), OrderedDict([('id', 'ripple')])]
        result = newcomers.get_latest_result("max_uuid")
        self.assertListEqual(result, ['bitcoin', 'ethereum', 'ripple'])

    @patch("serverless_crypto_analysis.lambda_function.newcomers.run_query")
    def test_tail_results(self, m_query):
        m_query.return_value = [OrderedDict([('id', 'iota')]), OrderedDict([('id', 'ethereum')]), OrderedDict([('id', 'ripple')])]
        result = newcomers.get_tail_results()
        self.assertListEqual(result, ['iota', 'ethereum', 'ripple'])

    @patch("serverless_crypto_analysis.lambda_function.newcomers.get_tail_results")
    @patch("serverless_crypto_analysis.lambda_function.newcomers.get_latest_result")
    @patch("serverless_crypto_analysis.lambda_function.newcomers.get_max_uuid")
    def test_handler(self, m_max_uuid, m_latest, m_tail):
        m_max_uuid.return_value = "max_uuid"
        m_latest.return_value = ['bitcoin', 'ethereum', 'ripple']
        m_tail.return_value = ['iota', 'ethereum', 'ripple']
        result = newcomers.lambda_handler(event=None, context=None)
        self.assertEqual(result, ['bitcoin'])