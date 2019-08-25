import unittest
from serverless_crypto_analysis.get_coinmarketcap_data import handler

class MyTestCase(unittest.TestCase):
    def test_get_coinmarketcap_data_and_dump_to_s3(self):

        result = handler({})

if __name__ == '__main__':
    unittest.main()
