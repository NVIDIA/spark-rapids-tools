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
    """Randomly split all rows into 'train' and 'val' sets.

    Parameters
    ----------
    features: pd.DataFrame
        Input dataframe
    seed: int
        Seed for random number generator
    val_pct: float
        Percentage of all rows to use for validation

    Returns
    -------
    pd.DataFrame
    """
    seed = kwargs.get('seed', 0)
    val_pct = kwargs.get('val_pct', 0.2)

    if 'split' in features.columns:
        # if 'split' already present, just modify the NaN rows
        # otherwise, modify all rows
        indices = features.index[features['split'].isna()].tolist()
    else:
        indices = features.index.tolist()

    num_rows = len(indices)
    random.Random(seed).shuffle(indices)

    # split remaining rows into train/val sets
    features.loc[indices[0: int(val_pct * num_rows)], 'split'] = 'val'
    features.loc[indices[int(val_pct * num_rows):], 'split'] = 'train'
    return features
