import os
import unittest
from unittest.mock import patch

from moto import mock_s3
import awswrangler as wr
from serverless_crypto_analysis.lambda_function import filter_for_newcomers
from serverless_crypto_analysis.utils import s3_utils
from serverless_crypto_analysis.utils.s3_utils import build_s3_url

dir_path = os.path.dirname(os.path.realpath(__file__))


class TestFilterForNewcomers(unittest.TestCase):

    # s3://serverless-crypto-analysis-stg/raw/year=2020/month=4/day=18/1587170541.json
    bucket = "bucket"
    source_key = "raw/year=2020/month=4/day=18/1587170541.json"
    destination_key = "stg/filter_for_newcomers"

    @mock_s3
    def test_lambda_handler_write_if_destination_does_not_exist(self) -> None:

        # os.environ["AWS_PROFILE"] = "serverless"
        os.environ["DESTINATION_BUCKET"] = TestFilterForNewcomers.bucket
        os.environ["DESTINATION_KEY"] = TestFilterForNewcomers.destination_key
        s3_utils.create_bucket(TestFilterForNewcomers.bucket)
        test_file_path = os.path.join(
            dir_path, "resources", "test_filter_for_newcomers.json"
        )
        test_file = TestFilterForNewcomers.read_local_object(test_file_path)
        s3_utils.put_s3_object(test_file, TestFilterForNewcomers.bucket, TestFilterForNewcomers.source_key)
        filter_for_newcomers.lambda_handler(
            event=TestFilterForNewcomers.build_event(bucket=TestFilterForNewcomers.bucket, key=TestFilterForNewcomers.source_key),
            context=None,
        )
        objects = s3_utils.get_objects_in_bucket(TestFilterForNewcomers.bucket)
        df = wr.s3.read_parquet(build_s3_url(TestFilterForNewcomers.bucket, TestFilterForNewcomers.destination_key))
        self.assertEqual(len(objects), 2)
        self.assertListEqual(df.columns.tolist(), ['id', 'rank', 'uuid'])
        self.assertEqual(df.shape, (200, 3))

    @mock_s3
    def test_lambda_handler_write_if_destination_does_exist(self) -> None:

        # os.environ["AWS_PROFILE"] = "serverless"
        os.environ["DESTINATION_BUCKET"] = TestFilterForNewcomers.bucket
        os.environ["DESTINATION_KEY"] = TestFilterForNewcomers.destination_key
        s3_utils.create_bucket(TestFilterForNewcomers.bucket)
        test_file_path = os.path.join(
            dir_path, "resources", "test_filter_for_newcomers.json"
        )
        test_file = TestFilterForNewcomers.read_local_object(test_file_path)
        s3_utils.put_s3_object(test_file, TestFilterForNewcomers.bucket, TestFilterForNewcomers.source_key)
        filter_for_newcomers.lambda_handler(
            event=TestFilterForNewcomers.build_event(bucket=TestFilterForNewcomers.bucket, key=TestFilterForNewcomers.source_key),
            context=None,
        )
        filter_for_newcomers.lambda_handler(
            event=TestFilterForNewcomers.build_event(bucket=TestFilterForNewcomers.bucket, key=TestFilterForNewcomers.source_key),
            context=None,
        )
        df = wr.s3.read_parquet(build_s3_url(TestFilterForNewcomers.bucket, TestFilterForNewcomers.destination_key))
        self.assertListEqual(df.columns.tolist(), ['id', 'rank', 'uuid'])
        self.assertEqual(df.shape, (400, 3))


    @staticmethod
    def read_local_object(path):
        return open(path, "rb")

    @staticmethod
    def build_event(bucket, key):
        return {
          "Records": [
            {
              "eventVersion": "2.1",
              "eventSource": "aws:s3",
              "awsRegion": "eu-central-1",
              "eventTime": "2020-04-18T12:38:39.403Z",
              "eventName": "ObjectCreated:Copy",
              "userIdentity": {
                "principalId": "A396E8UXVSR0H4"
              },
              "requestParameters": {
                "sourceIPAddress": "94.224.187.252"
              },
              "responseElements": {
                "x-amz-request-id": "0AC77FDBEFEA3BAC",
                "x-amz-id-2": "1SmCBHK0STFrgk1UNAEmM6g/JSAao2VE15oAMXTV/lH5dEH4nD3S3xlX6sljrjtWQKKTJHexArQxpDN+P4+y+ruHI0irxOb8"
              },
              "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": "f352e60b-9f7e-4995-b3aa-c9568fc4eda8",
                "bucket": {
                  "name": bucket,
                  "ownerIdentity": {
                    "principalId": "A396E8UXVSR0H4"
                  },
                  "arn": "arn:aws:s3:::serverless-crypto-analysis-stg"
                },
                "object": {
                  "key": key,
                  "size": 127289,
                  "eTag": "78205c0cae4d98f5cb30c89f64e311b4",
                  "sequencer": "005E9AF4D4F8FAF370"
                }
              }
            }
          ]
        }

if __name__ == "__main__":
    unittest.main()
