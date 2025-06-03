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

"""Unit tests for split functions."""

import pytest  # pylint: disable=import-error
import pandas as pd
import numpy as np
from spark_rapids_tools.tools.qualx.split_functions.split_random import split_function as split_random
from spark_rapids_tools.tools.qualx.split_functions.split_all_test import split_function as split_all_test
from spark_rapids_tools.tools.qualx.split_functions.split_train_val import split_function as split_train_val
from spark_rapids_tools.tools.qualx.split_functions.split_stratified import split_function as split_stratified

from ..conftest import SparkRapidsToolsUT


@pytest.fixture(name='sample_dataframe')
def _sample_dataframe():
    """Create a sample dataframe for testing."""
    n = 1000
    return pd.DataFrame({
        'feature1': np.random.rand(n),
        'feature2': np.random.rand(n),
        'Duration_speedup': np.random.rand(n) * 2,  # Values between 0 and 2
        'duration_sum_speedup': np.random.rand(n) * 2  # Values between 0 and 2
    })


class TestSplitFunctions(SparkRapidsToolsUT):
    """Test class for split functions."""

    def test_split_random(self, sample_dataframe):
        """Test random split function."""
        # Test with default parameters
        result = split_random(sample_dataframe)
        assert 'split' in result.columns
        assert set(result['split'].unique()) == {'train', 'val', 'test'}
        assert len(result) == len(sample_dataframe)

        counts = result.groupby('split').size()
        assert counts['test'] / len(result) == pytest.approx(0.2)
        assert counts['val'] / len(result) == pytest.approx(0.2)
        assert counts['train'] / len(result) == pytest.approx(0.6)

        # Test with custom parameters
        result = split_random(sample_dataframe, seed=42, test_pct=0.3, val_pct=0.2)
        counts = result.groupby('split').size()
        assert counts['test'] / len(result) == pytest.approx(0.3)
        assert counts['val'] / len(result) == pytest.approx(0.2)
        assert counts['train'] / len(result) == pytest.approx(0.5)

    def test_split_all_test(self, sample_dataframe):
        """Test all-test split function."""
        result = split_all_test(sample_dataframe)
        assert 'split' in result.columns
        assert all(result['split'] == 'test')
        assert len(result) == len(sample_dataframe)

    def test_split_train_val(self, sample_dataframe):
        """Test train-val split function."""
        # Test with default parameters
        result = split_train_val(sample_dataframe)
        assert 'split' in result.columns
        assert set(result['split'].unique()) == {'train', 'val'}
        assert len(result) == len(sample_dataframe)

        counts = result.groupby('split').size()
        assert counts['val'] / len(result) == pytest.approx(0.2)
        assert counts['train'] / len(result) == pytest.approx(0.8)

        # reset split column, since this split function will only modify empty rows
        sample_dataframe.drop(columns=['split'], inplace=True)

        # Test with custom parameters
        result = split_train_val(sample_dataframe, seed=42, val_pct=0.5)
        counts = result.groupby('split').size()
        assert counts['val'] / len(result) == pytest.approx(0.5)
        assert counts['train'] / len(result) == pytest.approx(0.5)

    def test_split_stratified(self, sample_dataframe):
        """Test stratified split function."""
        # Test with default parameters
        result = split_stratified(sample_dataframe)
        assert 'split' in result.columns
        assert set(result['split'].unique()) == {'train', 'val', 'test'}

        # Test with custom parameters
        result = split_stratified(
            sample_dataframe,
            seed=42,
            threshold=1.0,
            test_pct=0.3,
            val_pct=0.3,
            label_col='duration_sum_speedup'
        )
        assert 'split' in result.columns
        assert set(result['split'].unique()) == {'train', 'val', 'test'}

        # Test split proportions for positive and negative cases
        df = result.copy()
        df['true_positive'] = df['duration_sum_speedup'] > 1.0

        for is_positive in [True, False]:
            subset = df[df['true_positive'] == is_positive]
            counts = subset.groupby('split').size()
            total = len(subset)

            if total > 0:  # Only check if we have samples in this category
                assert counts['train'] / total == pytest.approx(0.4, abs=0.05)
                assert counts['val'] / total == pytest.approx(0.3, abs=0.05)
                assert counts['test'] / total == pytest.approx(0.3, abs=0.05)

    def test_split_random_reproducibility(self, sample_dataframe):
        """Test that random split produces same results with same seed."""
        sample1 = sample_dataframe.copy()
        result1 = split_random(sample1, seed=42)
        sample2 = sample_dataframe.copy()
        result2 = split_random(sample2, seed=42)
        pd.testing.assert_series_equal(result1['split'], result2['split'])

        # test different seed
        result2 = split_random(sample_dataframe, seed=1234)
        assert not result1['split'].equals(result2['split'])

    def test_split_train_val_reproducibility(self, sample_dataframe):
        """Test that train-val split produces same results with same seed."""
        sample1 = sample_dataframe.copy()
        result1 = split_train_val(sample1, seed=42)
        sample2 = sample_dataframe.copy()
        result2 = split_train_val(sample2, seed=42)
        pd.testing.assert_series_equal(result1['split'], result2['split'])

        # test different seed
        result2 = split_train_val(sample_dataframe, seed=1234)
        assert not result1['split'].equals(result2['split'])

    def test_split_stratified_reproducibility(self, sample_dataframe):
        """Test that stratified split produces same results with same seed."""
        sample1 = sample_dataframe.copy()
        result1 = split_stratified(sample1, seed=42)
        sample2 = sample_dataframe.copy()
        result2 = split_stratified(sample2, seed=42)
        pd.testing.assert_series_equal(result1['split'], result2['split'])

        # test different seed
        result2 = split_stratified(sample_dataframe, seed=1234)
        assert not result1['split'].equals(result2['split'])
