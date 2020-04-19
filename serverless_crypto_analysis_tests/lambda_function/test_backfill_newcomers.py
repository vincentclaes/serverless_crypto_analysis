import os
import unittest
from collections import OrderedDict

import boto3
import pandas as pd
from mock import patch
from moto import mock_s3

from serverless_crypto_analysis.lambda_function import backfill_newcomers
from serverless_crypto_analysis.utils.s3_utils import (create_bucket,
                                                       get_object_from_s3,
                                                       get_objects_in_bucket)

dir_path = os.path.dirname(os.path.realpath(__file__))


class TestNewcomers(unittest.TestCase):

    @patch("serverless_crypto_analysis.lambda_function.backfill_newcomers.trigger_lambda")
    @patch("serverless_crypto_analysis.lambda_function.backfill_newcomers.get_distinct_uuid")
    def test_handler_happy_flow(self, m_uuid, m_trigger):
        m_uuid.return_value = pd.read_csv(os.path.join(
            dir_path, "resources", "test_backfill_newcomers.csv"
        ))
        os.environ["YEAR"] = "2020"
        os.environ["MONTH"] = "4"
        os.environ["DAY"] = "1"
        os.environ["LOOKBACK_PERIOD"] = "86400"
        backfill_newcomers.lambda_handler(None, None)
