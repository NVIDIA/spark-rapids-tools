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

"""Default split functions."""

import random
import pandas as pd


def split_function(features: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """Randomly split all rows into test, val, and train sets.

    Parameters
    ----------
    features: pd.DataFrame
        Input dataframe
    seed: int
        Seed for random number generator
    test_pct: float
        Percentage of all rows to use for test
    val_pct: float
        Percentage of remaining non-test rows to use for validation

    Returns
    -------
    pd.DataFrame
    """
    seed = kwargs.get('seed', 0)
    test_pct = kwargs.get('test_pct', 0.2)
    val_pct = kwargs.get('val_pct', 0.2)

    num_rows = len(features)
    indices = list(range(num_rows))

    random.Random(seed).shuffle(indices)

    # first test_pct rows are test
    features.loc[indices[0: int(test_pct * num_rows)], 'split'] = 'test'

    # next val_pct rows are val
    features.loc[
        indices[int(test_pct * num_rows): int((test_pct + val_pct) * num_rows)],
        'split',
    ] = 'val'

    # remaining rows are train
    features.loc[indices[int((test_pct + val_pct) * num_rows):], 'split'] = 'train'
    return features
