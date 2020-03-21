import os
import unittest

from moto import mock_s3
from unittest.mock import patch
from serverless_crypto_analysis.lambda_function import filter_for_newcomers
from serverless_crypto_analysis.utils import s3_utils

dir_path = os.path.dirname(os.path.realpath(__file__))


class TestFilterForNewcomers(unittest.TestCase):
    bucket = "bucket"
    key = "1584200540.json"

    @mock_s3
    @patch("serverless_crypto_analysis.lambda_function.filter_for_newcomers.write_to_parquet")
    def test_lambda_handler(self, m_write) -> None:
        s3_utils.create_bucket(TestFilterForNewcomers.bucket)
        test_file_path = os.path.join(
            dir_path, "resources", "test_filter_for_newcomers.json"
        )
        test_file = TestFilterForNewcomers.read_local_object(test_file_path)
        s3_utils.put_s3_object(test_file, TestFilterForNewcomers.bucket, TestFilterForNewcomers.key)
        result = filter_for_newcomers.lambda_handler(
            event=TestFilterForNewcomers.build_event(bucket=TestFilterForNewcomers.bucket, key=TestFilterForNewcomers.key),
            context=None,
        )
        objects = s3_utils.get_objects_in_bucket(TestFilterForNewcomers.bucket)
        key = objects[0]["Key"]
        newcomer = s3_utils.get_object_from_s3(TestFilterForNewcomers.bucket, key)
        self.assertTrue(newcomer, "bitcoin")

    @staticmethod
    def read_local_object(path):
        return open(path, "rb")

    @staticmethod
    def build_event(bucket, key):
        return {
            "Records": [
                {
                    "eventVersion": "2.0",
                    "eventTime": "1970-01-01T00:00:00.000Z",
                    "requestParameters": {"sourceIPAddress": "127.0.0.1"},
                    "s3": {
                        "configurationId": "testConfigRule",
                        "object": {
                            "eTag": "0123456789abcdef0123456789abcdef",
                            "sequencer": "0A1B2C3D4E5F678901",
                            "key": key,
                            "size": 1024,
                        },
                        "bucket": {
                            "arn": "arn:aws:s3:::{}".format(bucket),
                            "name": bucket,
                            "ownerIdentity": {"principalId": "EXAMPLE"},
                        },
                        "s3SchemaVersion": "1.0",
                    },
                    "responseElements": {
                        "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH",
                        "x-amz-request-id": "EXAMPLE123456789",
                    },
                    "awsRegion": "us-east-1",
                    "eventName": "ObjectCreated:Put",
                    "userIdentity": {"principalId": "EXAMPLE"},
                    "eventSource": "aws:s3",
                }
            ]
        }


if __name__ == "__main__":
    unittest.main()
