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
import glob
from pathlib import Path

import pandas as pd
import pytest  # pylint: disable=import-error

from spark_rapids_tools.tools.qualx.preprocess import (
    load_datasets,
    expected_raw_features,
    impute
)
from ..conftest import SparkRapidsToolsUT


class TestPreprocess(SparkRapidsToolsUT):
    """Test class for qualx_preprocess module"""

    def get_jar_path(self):
        # get path to main repository
        # note: this only works in dev environment and is used to populate the qualx_cache
        repo_root = Path(__file__).parent.parent.parent.parent
        jar_path = repo_root / 'core' / 'target' / 'rapids-4-spark-tools_*-SNAPSHOT.jar'
        jar_paths = glob.glob(str(jar_path))
        return jar_paths[0] if jar_paths else ''

    @pytest.mark.parametrize('label', ['Duration', 'duration_sum'])
    def test_load_datasets(self, get_ut_data_dir, label):
        # set up environment variables used during preprocessing
        os.environ['QUALX_DATA_DIR'] = str(get_ut_data_dir / 'eventlogs')
        os.environ['QUALX_CACHE_DIR'] = str(get_ut_data_dir / 'qualx_cache')
        os.environ['QUALX_LABEL'] = label
        jar_path = self.get_jar_path()
        if jar_path:
            os.environ['SPARK_RAPIDS_TOOLS_JAR'] = jar_path

        # remove any preprocessed.parquet files if present, but keep any profiler CSV files
        # this exercises the preprocessing code, without having to re-run the profiler tool
        cache_dir = Path(os.environ['QUALX_CACHE_DIR'])
        if cache_dir.exists():
            preprocessed_files = glob.glob(str(cache_dir) + '/**/preprocessed.parquet')
            for f in preprocessed_files:
                os.remove(f)

        # if running in a tox virtual environment, set SPARK_HOME to the venv's pyspark path
        venv_path = os.environ.get('VIRTUAL_ENV', None)
        if venv_path:
            spark_home = glob.glob(f'{venv_path}/lib/*/site-packages/pyspark')
            if spark_home:
                os.environ['SPARK_HOME'] = spark_home[0]

        # Load the datasets
        datasets_dir = str(get_ut_data_dir / 'datasets')

        all_datasets, profile_df = load_datasets(datasets_dir)

        # Basic assertions
        assert isinstance(all_datasets, dict)
        assert 'nds_local' in all_datasets

        assert isinstance(profile_df, pd.DataFrame)
        assert not profile_df.empty
        # assert profile_df.shape == (194, 127)
        assert set(profile_df.columns) == expected_raw_features
        assert label in profile_df.columns

    def test_impute(self):
        # Test impute function
        input_df = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': [4, 5, 6]
        })

        imputed_df = impute(input_df)
        df_columns = set(imputed_df.columns)

        # should not have extra columns
        assert 'col1' not in df_columns
        assert 'col2' not in df_columns

        # should have all expected raw features
        assert df_columns == expected_raw_features

        # fraction_supported should be 1.0
        assert imputed_df['fraction_supported'].iloc[0] == 1.0

        # all other columns should be 0.0
        assert imputed_df[list(df_columns - {'fraction_supported'})].iloc[0].sum() == 0.0
