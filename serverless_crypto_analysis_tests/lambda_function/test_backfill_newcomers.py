import os
import unittest
from collections import OrderedDict
import pandas as pd
import boto3
from mock import patch
from moto import mock_s3
from serverless_crypto_analysis.utils.s3_utils import get_object_from_s3
from serverless_crypto_analysis.utils.s3_utils import get_objects_in_bucket
from serverless_crypto_analysis.utils.s3_utils import create_bucket
from serverless_crypto_analysis.lambda_function import backfill_newcomers

dir_path = os.path.dirname(os.path.realpath(__file__))


class TestNewcomers(unittest.TestCase):

    @patch("serverless_crypto_analysis.lambda_function.backfill_newcomers.get_distinct_uuid")
    def test_handler(self, m_uuid):
        m_uuid.return_value = pd.read_csv(os.path.join(
            dir_path, "resources", "test_backfill_newcomers.csv"
        ))
        backfill_newcomers.lambda_handler(None, None)
