import os
import unittest
from collections import OrderedDict

import boto3
from mock import patch
from moto import mock_s3
from serverless_crypto_analysis.utils.s3_utils import get_object_from_s3
from serverless_crypto_analysis.utils.s3_utils import get_objects_in_bucket
from serverless_crypto_analysis.utils.s3_utils import create_bucket
from serverless_crypto_analysis.lambda_function import newcomers

DIR_PATH = os.path.dirname(os.path.realpath(__file__))


class TestNewcomers(unittest.TestCase):
    @patch("serverless_crypto_analysis.lambda_function.newcomers.run_query")
    def test_max_uuid(self, m_query):
        m_query.return_value = [OrderedDict([("_col0", "1575396043")])]
        result = newcomers.get_max_uuid("db", "table")
        self.assertEqual(result, "1575396043")

    @patch("serverless_crypto_analysis.lambda_function.newcomers.run_query")
    def test_latest_results(self, m_query):
        m_query.return_value = [
            OrderedDict([("id", "bitcoin")]),
            OrderedDict([("id", "ethereum")]),
            OrderedDict([("id", "ripple")]),
        ]
        result = newcomers.get_latest_result("db", "table", "max_uuid", "100")
        self.assertListEqual(result, ["bitcoin", "ethereum", "ripple"])

    @patch("serverless_crypto_analysis.lambda_function.newcomers.run_query")
    def test_tail_results(self, m_query):
        m_query.return_value = [
            OrderedDict([("id", "iota")]),
            OrderedDict([("id", "ethereum")]),
            OrderedDict([("id", "ripple")]),
        ]
        result = newcomers.get_tail_results("db", "table", "38484892", "100")
        self.assertListEqual(result, ["iota", "ethereum", "ripple"])

    @mock_s3
    @patch("serverless_crypto_analysis.lambda_function.newcomers.get_tail_results")
    @patch("serverless_crypto_analysis.lambda_function.newcomers.get_latest_result")
    @patch("serverless_crypto_analysis.lambda_function.newcomers.get_max_uuid")
    def test_handler(self, m_max_uuid, m_latest, m_tail):
        bucket = "bucket"
        os.environ["ATHENA_DB"] = "db"
        os.environ["ATHENA_TABLE"] = "table"
        os.environ["BUCKET_DATA"] = bucket
        os.environ["KEY_DATA"] = "stg/newcomers"
        os.environ["RANK"] = '["100", "200"]'

        create_bucket(bucket)
        m_max_uuid.return_value = "max_uuid"
        m_latest.return_value = ["bitcoin", "ethereum", "ripple"]
        m_tail.return_value = ["iota", "ethereum", "ripple"]
        newcomers.lambda_handler(event=None, context=None)
        objects = get_objects_in_bucket(bucket)
        key = objects[0]["Key"]
        newcomer = get_object_from_s3(bucket, key)
        self.assertTrue(newcomer, "bitcoin")
