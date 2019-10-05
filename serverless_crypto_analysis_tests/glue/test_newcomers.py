import json
import os
import unittest

import pandas as pd
from mock import patch
DIR_PATH = os.path.dirname(os.path.realpath(__file__))
from serverless_crypto_analysis.glue.newcomers import check_for_newcomers

class TestNewcomers(unittest.TestCase):

    def setUp(self):
        self.df_last = pd.read_csv(os.path.join(DIR_PATH, 'test_files', 'test_newcomers_df_last'), index_col=0)
        self.df_last['id'][0] = "my_coin"
        self.df_tail = pd.read_csv(os.path.join(DIR_PATH, 'test_files', 'test_newcomers_df_tail'), index_col=0)


    def test_get_newcomers(self):
        newcomers = check_for_newcomers(None, None)
        newcomers

