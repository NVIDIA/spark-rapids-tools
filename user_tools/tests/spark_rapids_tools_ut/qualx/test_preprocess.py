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

"""Test qualx_preprocess module"""
import os
import json
import tempfile
from unittest.mock import patch

import pandas as pd

from spark_rapids_tools.tools.qualx.config import get_config
from spark_rapids_tools.tools.qualx.preprocess import (
    expected_raw_features,
    impute,
    get_alignment,
    get_featurizers,
    get_modifiers,
    infer_app_meta,
    load_qtool_execs,
    load_datasets
)
from ..conftest import SparkRapidsToolsUT


class TestPreprocess(SparkRapidsToolsUT):
    """Test class for qualx_preprocess module"""
    def test_impute(self):
        # Test impute function
        input_df = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': [4, 5, 6]
        })

        expected_features = expected_raw_features()
        imputed_df = impute(input_df, expected_features)
        df_columns = set(imputed_df.columns)

        # should not have extra columns
        assert 'col1' not in df_columns
        assert 'col2' not in df_columns

        # should have all expected raw features
        assert df_columns == expected_features

        # fraction_supported should be 1.0
        assert imputed_df['fraction_supported'].iloc[0] == 1.0

        # all other columns should be 0.0
        assert imputed_df[list(df_columns - {'fraction_supported'})].iloc[0].sum() == 0.0

    def test_get_alignment(self):
        # Test get_alignment function
        with patch('spark_rapids_tools.tools.qualx.preprocess.get_config') as mock_config:
            # Create temporary alignment CSV file
            test_alignment = pd.DataFrame({'appId': ['app1', 'app2'], 'sqlId': ['sql1', 'sql2']})
            with tempfile.NamedTemporaryFile(prefix='alignment_', suffix='.csv', mode='w') as f:
                test_alignment.to_csv(f.name, index=False)

                mock_config.return_value.alignment_file = f.name

                # Call function
                align_df = get_alignment()

                # Verify results
                assert isinstance(align_df, pd.DataFrame)
                assert len(align_df) == 2
                assert list(align_df.columns) == ['appId', 'sqlId']
                assert list(align_df['appId']) == ['app1', 'app2']
                assert list(align_df['sqlId']) == ['sql1', 'sql2']

    def test_get_featurizers(self):
        # Test get_featurizers function
        get_config(reload=True)
        result = get_featurizers()

        # Verify results
        assert isinstance(result, list)
        assert len(result) >= 2
        assert all(callable(f.extract_raw_features) for f in result)

        # Test caching
        with patch('spark_rapids_tools.tools.qualx.util.load_plugin') as mock_load_plugin:
            result2 = get_featurizers()
            mock_load_plugin.assert_not_called()
            assert result == result2

    def test_get_modifiers(self):
        # Test get_modifiers function
        with patch('spark_rapids_tools.tools.qualx.preprocess.get_config') as mock_config:
            mock_config.return_value.modifiers = ['align_sql_id.py']
            result = get_modifiers()

            # Verify results
            assert isinstance(result, list)
            assert len(result) >= 1
            assert all(callable(f.modify) for f in result)

            # Test caching
            with patch('spark_rapids_tools.tools.qualx.util.load_plugin') as mock_load_plugin:
                result2 = get_modifiers()
                mock_load_plugin.assert_not_called()
                assert result == result2

    def test_infer_app_meta(self):
        # Test infer_app_meta function
        with patch('spark_rapids_tools.tools.qualx.preprocess.find_eventlogs') as mock_find_eventlogs:
            # Setup mock eventlogs
            mock_find_eventlogs.return_value = [
                '/path/to/job1/eventlogs/CPU/app1',
                '/path/to/job1/eventlogs/GPU/app2',
                '/path/to/job2/eventlogs/CPU/app3',
                '/path/to/job2/eventlogs/GPU/app4',
            ]

            # Call function
            result = infer_app_meta(['/path/to/job1', '/path/to/job2'])

            # Verify results
            assert isinstance(result, dict)
            assert len(result) == 4
            assert result['app1'] == {
                'jobName': 'job1',
                'runType': 'CPU',
                'scaleFactor': 1
            }
            assert result['app2'] == {
                'jobName': 'job1',
                'runType': 'GPU',
                'scaleFactor': 1
            }
            assert result['app3'] == {
                'jobName': 'job2',
                'runType': 'CPU',
                'scaleFactor': 1
            }
            assert result['app4'] == {
                'jobName': 'job2',
                'runType': 'GPU',
                'scaleFactor': 1
            }

    def test_load_qtool_execs(self):
        # Test load_qtool_execs function
        # Create test data
        test_data = pd.DataFrame([
            ['app1', 'sql1', 'node1', True, '', 'Exec1'],
            ['app1', 'sql2', 'node2', False, 'IgnoreNoPerf', 'Exec2'],
            ['app2', 'sql1', 'node1', False, '', 'WholeStageCodegen3'],
            ['app2', 'sql1', 'node2', False, '', 'Exec4']
        ], columns=['App ID', 'SQL ID', 'SQL Node Id', 'Exec Is Supported', 'Action', 'Exec Name'])

        # Create temporary CSV file
        with tempfile.NamedTemporaryFile(suffix='.csv', mode='w') as f:
            test_data.to_csv(f.name, index=False)

            # Call function
            result = load_qtool_execs([f.name])

            # Verify results
            assert isinstance(result, pd.DataFrame)
            assert list(result.columns) == ['App ID', 'SQL ID', 'SQL Node Id', 'Exec Is Supported']
            assert len(result) == 4
            assert result['Exec Is Supported'].tolist() == [True, True, True, False]

    def test_load_datasets(self):
        """Test load_datasets function"""
        get_config(reload=True)
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test dataset structure
            dataset_dir = os.path.join(temp_dir, 'datasets', 'onprem')
            os.makedirs(dataset_dir)

            # Create test JSON file
            test_json = {
                'dataset1': {
                    'eventlogs': ['/path/to/eventlog1'],
                    'platform': 'onprem',
                    'app_meta': {
                        'app1': {
                            'runType': 'CPU',
                            'scaleFactor': 1
                        }
                    }
                }
            }

            json_path = os.path.join(dataset_dir, 'dummy.json')
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(test_json, f)

            # Create dummy preprocessed.parquet cache file
            test_profile = pd.DataFrame([
                ['dataset1', 'app1', 'sql1', 100, 0.8],
                ['dataset1', 'app1', 'sql2', 150, 0.6],
                ['dataset2', 'app2', 'sql1', 200, 0.9],
                ['dataset2', 'app2', 'sql2', 180, 0.7]
            ], columns=['appName', 'appId', 'sqlID', 'Duration', 'fraction_supported'])
            profile_dir = os.path.join(get_config().cache_dir, 'onprem')
            os.makedirs(profile_dir, exist_ok=True)
            test_profile.to_parquet(os.path.join(profile_dir, 'preprocessed.parquet'), index=False)

            # Call function
            datasets, profile_df = load_datasets(dataset_dir)

            # Verify results
            assert isinstance(datasets, dict)
            assert 'dataset1' in datasets
            assert datasets['dataset1']['platform'] == 'onprem'

            assert isinstance(profile_df, pd.DataFrame)
            assert not profile_df.empty
            assert len(profile_df) == 2  # only 2 rows correspond to dataset1
