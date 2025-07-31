# Copyright (c) 2025, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Test data utils functionalities"""

from io import StringIO
from unittest.mock import patch, mock_open

import unittest
import pandas as pd

from spark_rapids_tools.utils.data_utils import DataUtils, LoadDFResult


class TestDataUtils(unittest.TestCase):
    """
    A unit test for the data-utils
    """

    def test_read_dataframe_basic(self):
        csv_data = 'a,b\n1,2\n3,4'
        expected_df = pd.DataFrame({'a': [1, 3], 'b': [2, 4]})
        df = DataUtils.read_dataframe(StringIO(csv_data))
        pd.testing.assert_frame_equal(df, expected_df)

    def test_read_dataframe_with_column_mapping(self):
        csv_data = 'x,y\n5,6\n7,8'
        expected_df = pd.DataFrame({'a': [5, 7], 'b': [6, 8]})
        df = DataUtils.read_dataframe(StringIO(csv_data), map_columns={'x': 'a', 'y': 'b'})
        pd.testing.assert_frame_equal(df, expected_df)

    def test_read_dataframe_with_kwargs(self):
        """
        Tests that read_dataframe forwards the pandas read-csv kwargs like selecting specific
        columns
        """
        csv_data = 'x,y\n5,6\n7,8'
        expected_df = pd.DataFrame({'x': [5, 7]})
        df = DataUtils.read_dataframe(
            StringIO(csv_data), usecols=['x'])
        pd.testing.assert_frame_equal(df, expected_df)

    def test_load_pd_df_success(self):
        csv_data = 'x,y\n9,10\n11,12'
        mock_open_func = mock_open(read_data=csv_data)
        expected_df = pd.DataFrame({'x': [9, 11], 'y': [10, 12]})

        with patch('builtins.open', mock_open_func):
            result = DataUtils.load_pd_df('mock.csv')
            self.assertIsInstance(result, LoadDFResult)
            self.assertTrue(result.success)
            self.assertFalse(result.fallen_back)
            self.assertIsNone(result.load_error)
            pd.testing.assert_frame_equal(result.data, expected_df)

    def test_load_pd_df_fallback(self):
        def default_df_cb():
            return pd.DataFrame({'a': [0], 'b': [0]})

        with patch('pandas.read_csv', side_effect=FileNotFoundError('File missing')):
            result = DataUtils.load_pd_df('nonexistent.csv', default_cb=default_df_cb)
            self.assertTrue(result.success)
            self.assertTrue(result.fallen_back)
            # check that the load-error has the correct cause for exception.
            self.assertIsInstance(result.get_fail_cause(), FileNotFoundError)
            pd.testing.assert_frame_equal(result.data, default_df_cb())

    def test_load_pd_df_with_mapping_and_kwargs(self):
        csv_data = 'col1;col2\n1;2\n3;4'
        expected_df = pd.DataFrame({'a': [1, 3], 'b': [2, 4]})
        mock_open_func = mock_open(read_data=csv_data)

        with patch('builtins.open', mock_open_func):
            result = DataUtils.load_pd_df(
                f_path='mock.csv',
                map_columns={'col1': 'a', 'col2': 'b'},
                read_csv_kwargs={'sep': ';'}
            )
            pd.testing.assert_frame_equal(result.data, expected_df)
