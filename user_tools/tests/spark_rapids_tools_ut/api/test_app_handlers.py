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

"""Module to test functionality of the app_handlers"""

import unittest
from io import StringIO

import pandas as pd

from spark_rapids_tools.api_v1 import AppHandler


class TestAppHandlers(unittest.TestCase):
    """
    A unit test for the app-handlers
    """


def test_add_fields_to_dataframe_basic():
    csv_data = 'a,b\n1,2\n3,4'
    original_df = pd.read_csv(StringIO(csv_data))
    handler = AppHandler(_app_id='app-00',
                         _attempt_id=1,
                         _app_name='app_name-00',
                         eventlog_path='/path/to/file')
    # User wants to map 'appID' and 'name' to columns 'APP ID' and 'App Name'
    mapping = {'app_id': 'App ID', 'app_name': 'App Name'}
    result_df = handler.add_fields_to_dataframe(original_df, mapping)

    assert result_df.shape == (2, 4)
    assert result_df.columns[0] == 'App ID'
    assert result_df.columns[1] == 'App Name'
    expected_data = 'App ID,App Name,a,b\napp-00,app_name-00,1,2\napp-00,app_name-00,3,4'
    expected_df = pd.read_csv(StringIO(expected_data)).astype({'App ID': 'string', 'App Name': 'string'})
    # assert the dataTypes are correct (string and not object)
    assert result_df.dtypes[0] == 'string'
    assert result_df.dtypes[1] == 'string'
    # verify the entire dataframes are equal
    pd.testing.assert_frame_equal(expected_df, result_df)


def test_add_fields_to_dataframe_empty():
    """
    Test adding fields to an empty DataFrame.
    """
    # Define the schema using a dictionary where keys are column names and values are data types
    schema = {
        'col01': 'string',
        'col02': 'int64',
        'col03': 'string',
        'col04': 'float32'
    }

    # Create an empty DataFrame with the defined schema
    empty_df = pd.DataFrame({
        col: pd.Series(dtype=dtype)
        for col, dtype in schema.items()
    })

    handler = AppHandler(_app_id='app-00',
                         _attempt_id=1,
                         _app_name='app_name-00',
                         eventlog_path='/path/to/file')
    # User wants to map 'appID' and 'name' to columns 'APP ID' and 'App Name'
    mapping = {'app_id': 'App ID', 'attempt_id': 'Attempt ID', 'app_name': 'App Name'}
    result_df = handler.add_fields_to_dataframe(empty_df, mapping)
    assert result_df.shape == (0, 7)  # Should still be empty with 6 columns
    expected_schema = {
        'App ID': 'string',
        'Attempt ID': 'Int64',
        'App Name': 'string',
        'col01': 'string',
        'col02': 'int64',
        'col03': 'string',
        'col04': 'float32'
    }

    for col, dtype in expected_schema.items():
        assert result_df[col].dtype.name == dtype
