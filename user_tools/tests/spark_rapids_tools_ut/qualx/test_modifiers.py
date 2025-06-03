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

"""Unit tests for qualx modifiers."""

import pytest  # pylint: disable=import-error
import pandas as pd
from spark_rapids_tools.tools.qualx.config import get_config
from spark_rapids_tools.tools.qualx.modifiers.align_sql_id import (
    compute_alignment_from_raw_features,
    compute_alignment_from_app_pairs,
    modify
)
from ..conftest import SparkRapidsToolsUT


@pytest.fixture(name='raw_features')
def _raw_features():
    return pd.DataFrame(
        [
            # appName, runType, appId, sqlID, hash
            ('test1', 'CPU', 'app1', 1, 'hash1'),
            ('test1', 'CPU', 'app1', 2, 'hash2'),
            ('test1', 'GPU', 'app2', 1, 'hash1'),
            ('test1', 'GPU', 'app2', 2, 'hash3'),
            ('test2', 'CPU', 'app3', 1, 'hash4'),
            ('test2', 'GPU', 'app4', 2, 'hash4')
        ],
        columns=['appName', 'runType', 'appId', 'sqlID', 'hash']
    )


class TestModifiers(SparkRapidsToolsUT):
    """Test class for qualx_modifiers module"""
    def test_compute_alignment_from_raw_features(self, raw_features):
        alignment_df = compute_alignment_from_raw_features(raw_features)

        # Verify the structure of the output
        assert isinstance(alignment_df, pd.DataFrame)
        assert all(col in alignment_df.columns for col in ['appId_cpu', 'appId_gpu', 'sqlID_cpu', 'sqlID_gpu', 'hash'])

        # Verify specific alignments
        assert len(alignment_df) > 0

        assert alignment_df['appId_cpu'].iloc[0] == 'app1'
        assert alignment_df['appId_gpu'].iloc[0] == 'app2'
        assert alignment_df['sqlID_cpu'].iloc[0] == 1
        assert alignment_df['sqlID_gpu'].iloc[0] == 1
        assert alignment_df['hash'].iloc[0] == 'hash1'

        assert alignment_df['appId_cpu'].iloc[1] == 'app3'
        assert alignment_df['appId_gpu'].iloc[1] == 'app4'
        assert alignment_df['sqlID_cpu'].iloc[1] == 1
        assert alignment_df['sqlID_gpu'].iloc[1] == 2
        assert alignment_df['hash'].iloc[1] == 'hash4'

    def test_compute_alignment_from_app_pairs(self, raw_features):
        app_pairs = pd.DataFrame(
            [
                ('app1', 'app2'),
                ('app3', 'app4')
            ],
            columns=['appId_cpu', 'appId_gpu']
        )

        alignment_df = compute_alignment_from_app_pairs(raw_features, app_pairs)

        # Verify the structure of the output
        assert isinstance(alignment_df, pd.DataFrame)
        assert all(col in alignment_df.columns for col in ['appId_cpu', 'appId_gpu', 'sqlID_cpu', 'sqlID_gpu', 'hash'])

        # Verify specific alignments
        assert len(alignment_df) > 0
        assert alignment_df['appId_cpu'].iloc[0] == 'app1'
        assert alignment_df['appId_gpu'].iloc[0] == 'app2'
        assert alignment_df['sqlID_cpu'].iloc[0] == 1
        assert alignment_df['sqlID_gpu'].iloc[0] == 1
        assert alignment_df['hash'].iloc[0] == 'hash1'

    def test_modify_with_provided_alignment(self, raw_features):
        # Create sample alignment data using row-based representation
        alignment_df = pd.DataFrame(
            [
                ('app1', 'app2', 1, 1, 'hash1'),
                ('app3', 'app4', 1, 2, 'hash4')
            ],
            columns=['appId_cpu', 'appId_gpu', 'sqlID_cpu', 'sqlID_gpu', 'hash']
        )

        config = get_config(reload=True)

        raw_features['description'] = 'dummy'
        result = modify(raw_features, config, alignment_df)

        # Verify the structure of the output
        assert isinstance(result, pd.DataFrame)

        # Verify CPU row descriptions have been updated to the GPU appId
        cpu_rows = result[result['runType'] == 'CPU']
        assert all(cpu_rows['description'] == cpu_rows['appId'])

        # Verify GPU row descriptions have been updated to the CPU appId
        # Verify the alignment is correct
        assert all(result.loc[(result.appId == 'app2') & (result.hash == 'hash1'), 'sqlID'] == 1)
        assert all(result.loc[(result.appId == 'app2') & (result.hash == 'hash3'), 'sqlID'] == -1)
        assert all(result.loc[(result.appId == 'app4') & (result.hash == 'hash4'), 'sqlID'] == 1)
