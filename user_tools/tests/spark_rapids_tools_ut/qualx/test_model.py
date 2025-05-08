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

"""Test qualx_model module"""

import random

import pytest  # pylint: disable=import-error
import pandas as pd
import numpy as np

from xgboost import Booster

from spark_rapids_tools.tools.qualx.config import get_config
from spark_rapids_tools.tools.qualx.preprocess import expected_raw_features
from spark_rapids_tools.tools.qualx.model import (
    extract_model_features,
    predict,
    train,
    compute_shapley_values,
)
from spark_rapids_tools.tools.qualx.util import get_abs_path, load_plugin
from ..conftest import SparkRapidsToolsUT


class TestModel(SparkRapidsToolsUT):
    """Test class for qualx_model module"""
    @staticmethod
    def generate_test_data(label_col: str, seed: int = 0) -> pd.DataFrame:
        # Create test data with 200 rows (100 CPU/GPU pairs)
        np.random.seed(seed)
        random.seed(seed)

        # fill in random features and random labels
        fixed_features = set(['appName', 'description', 'runType', 'scaleFactor', 'sqlID'])
        random_features = expected_raw_features() - fixed_features
        features = {}
        for feature in random_features:
            features[feature] = np.random.rand(200)
        df = pd.DataFrame(features)
        df[label_col] = np.random.rand(200)

        # fill in fixed features
        df['appName'] = 'test_app'
        df['description'] = 'testing'
        df['scaleFactor'] = 1
        df.loc[0:99, 'sqlID'] = range(100)
        df.loc[0:99, 'runType'] = 'CPU'
        df.loc[100:199, 'sqlID'] = range(100)
        df.loc[100:199, 'runType'] = 'GPU'

        return df

    @pytest.mark.parametrize('label', ['Duration', 'duration_sum'])
    def test_train_and_predict(self, monkeypatch, label) -> None:
        # mock os.environ
        monkeypatch.setenv('QUALX_LABEL', label)
        get_config(reload=True)  # reload config

        # Create test data
        df = self.generate_test_data(label)

        # Extract features
        plugin = load_plugin(get_abs_path('split_random.py', 'split_functions'))
        features, feature_cols, label_col = extract_model_features(
            df, split_functions={'default': plugin.split_function}
        )

        # Assert label_col column exists
        assert label_col == f'{label}_speedup'
        assert label_col in features.columns

        # Assert label column is in features, but not in list of feature_cols
        assert label in features.columns
        assert label not in feature_cols

        model = train(features, feature_cols, label_col, n_trials=5)

        # Assert model is trained
        assert isinstance(model, Booster)
        assert model.num_boosted_rounds() > 0

        # Make predictions
        results = predict(model, features, feature_cols, label_col)

        # Assert predictions are made
        assert 'y_pred' in results.columns
        assert f'{label}_pred' in results.columns
        assert len(results) == 100

        # Compute SHAP values
        feature_importance, shap_values = compute_shapley_values(model, features)

        # Assert results
        assert isinstance(feature_importance, pd.DataFrame)
        assert isinstance(shap_values, pd.DataFrame)
        assert 'feature' in feature_importance.columns
        assert 'shap_value' in feature_importance.columns
        assert len(shap_values.columns) == len(feature_cols) + 1
