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
    """Randomly split into stratified train/val/test sets, trying to maintain balance between classes.

    Parameters
    ----------
    features: pd.DataFrame
        Input dataframe
    label_col: str
        Label column name
    threshold: float
        Threshold for true positive
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
    threshold = kwargs.get('threshold', 1.0)
    test_pct = kwargs.get('test_pct', 0.2)
    val_pct = kwargs.get('val_pct', 0.2)
    label_col = kwargs.get('label_col', 'Duration_speedup')

    # ensure 'split' column is present
    if 'split' not in features.columns:
        # add empty split column
        features['split'] = ''

    rand = random.Random(seed)

    if label_col in features.columns:  # QXS does not have a label column
        df = features.copy()
        # stratified split by positives / negatives
        df['true_positive'] = df[label_col] > threshold
        positives = df.loc[df['true_positive']]
        negatives = df.loc[~df['true_positive']]

        # get train/val/test split counts for positives
        num_positives = len(positives)
        num_test_positives = int(num_positives * test_pct)
        num_val_positives = int(num_positives * val_pct)
        num_train_positives = num_positives - num_test_positives - num_val_positives

        # shuffle and split positive indices
        indices = positives.index.tolist()
        rand.shuffle(indices)
        positives.loc[indices[:num_train_positives], 'split'] = 'train'
        positives.loc[indices[num_train_positives:num_train_positives + num_val_positives], 'split'] = 'val'
        positives.loc[indices[num_train_positives + num_val_positives:], 'split'] = 'test'

        # get train/val/test split counts for negatives
        num_negatives = len(negatives)
        num_test_negatives = int(num_negatives * test_pct)
        num_val_negatives = int(num_negatives * val_pct)
        num_train_negatives = num_negatives - num_test_negatives - num_val_negatives

        # shuffle and split negative indices
        indices = negatives.index.tolist()
        rand.shuffle(indices)
        negatives.loc[indices[:num_train_negatives], 'split'] = 'train'
        negatives.loc[indices[num_train_negatives:num_train_negatives + num_val_negatives], 'split'] = 'val'
        negatives.loc[indices[num_train_negatives + num_val_negatives:], 'split'] = 'test'

        # merge positives and negatives
        df.update(positives)
        df.update(negatives)
        features.update(df)

    return features
