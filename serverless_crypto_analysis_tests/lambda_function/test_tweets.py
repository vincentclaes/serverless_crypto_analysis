import unittest

from mock import patch
import json
from serverless_crypto_analysis.lambda_function import tweets
from serverless_crypto_analysis.lambda_function.tweets import Twython
from serverless_crypto_analysis.utils import s3_utils
from moto import mock_secretsmanager
from moto import mock_s3
import boto3
import os

dir_path = os.path.dirname(os.path.realpath(__file__))


class TestTweets(unittest.TestCase):
    bucket = "bucket"
    rank = 100
    key = "s3://serverless-crypto-analysis-prd/stg/newcomers/rank={}/2019-12-17T21:01:12.717000-qcash.json".format(
        rank
    )
    twitter_token = "twitter_token"
    coinmarketcap_token = "coinmarketcap_token"

    @staticmethod
    def read_local_object(path):
        return open(path, "rb")

    @classmethod
    def setUpClass(cls) -> None:
        pass

    @mock_s3
    @mock_secretsmanager
    @patch("serverless_crypto_analysis.lambda_function.tweets.get_coinmarketcap_tokens")
    @patch.object(Twython, "update_status")
    def test_run_several_ids(self, m_status, m_coinmarketcap) -> None:
        m_coinmarketcap.return_value = TestTweets.build_coinmarket_response()
        s3_utils.create_bucket(TestTweets.bucket)
        test_file_path = os.path.join(
            dir_path, "resources", "test_tweets_successfully.json"
        )
        test_file = TestTweets.read_local_object(test_file_path)
        s3_utils.put_s3_object(test_file, TestTweets.bucket, TestTweets.key)

        os.environ["TWITTER_TOKEN"] = TestTweets.twitter_token
        os.environ["COINMARKETCAP_TOKEN"] = TestTweets.coinmarketcap_token

        conn = boto3.client("secretsmanager")
        twitter_keys = {
            "app_key": "app-key",
            "app_secret": "app_secret",
            "oauth_token": "oauth_token",
            "oauth_token_secret": "oauth_token_secret",
        }
        conn.create_secret(
            Name=TestTweets.twitter_token, SecretString=json.dumps(twitter_keys)
        )
        conn.create_secret(Name=TestTweets.coinmarketcap_token, SecretString="some-key")
        tweets.lambda_handler(
            event=TestTweets.build_event(bucket=TestTweets.bucket, key=TestTweets.key),
            context=None,
        )

    @staticmethod
    def build_coinmarket_response():
        return {
            "status": {
                "timestamp": "2019-12-21T13:50:04.191Z",
                "error_code": 0,
                "elapsed": 5,
                "credit_count": 1,
            },
            "data": {
                "1": {
                    "urls": {
                        "website": ["https://bitcoin.org/"],
                        "technical_doc": ["https://bitcoin.org/bitcoin.pdf"],
                        "twitter": ["https://twitter.com/bitcoin"],
                        "reddit": ["https://reddit.com/r/bitcoin"],
                        "message_board": ["https://bitcointalk.org"],
                        "announcement": [],
                        "chat": [],
                        "explorer": [
                            "https://blockchain.coinmarketcap.com/chain/bitcoin",
                            "https://blockchain.info/",
                            "https://live.blockcypher.com/btc/",
                            "https://blockchair.com/bitcoin",
                            "https://explorer.viabtc.com/btc",
                        ],
                        "source_code": ["https://github.com/bitcoin/"],
                    },
                    "logo": "https://s2.coinmarketcap.com/static/img/coins/64x64/1.png",
                    "id": 1,
                    "name": "Bitcoin",
                    "symbol": "BTC",
                    "slug": "bitcoin",
                    "description": 'Bitcoin (BTC) is a consensus network that enables a new payment system and a completely digital currency. Powered by its users, it is a peer to peer payment network that requires no central authority to operate. On October 31st, 2008, an individual or group of individuals operating under the pseudonym "Satoshi Nakamoto" published the Bitcoin Whitepaper and described it as: "a purely peer-to-peer version of electronic cash, which would allow online payments to be sent directly from one party to another without going through a financial institution."',
                    "date_added": "2013-04-28T00:00:00.000Z",
                    "tags": ["mineable"],
                    "category": "coin",
                }
            },
        }

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
