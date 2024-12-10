# Copyright (c) 2024, NVIDIA CORPORATION.
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

""" Model training and prediction functions for QualX """

from typing import Callable, Mapping, Optional, List, Tuple
import json
import random
import shap
import numpy as np
import pandas as pd
import xgboost as xgb
from xgboost import Booster
from spark_rapids_tools.tools.qualx.preprocess import expected_raw_features
from spark_rapids_tools.tools.qualx.util import get_logger
# Import optional packages
try:
    import optuna
except ImportError:
    optuna = None

logger = get_logger(__name__)

FILTER_SPILLS = False  # remove queries with any disk/mem spill
LOG_LABEL = True  # use log(y) as target


# non-training features (and labels)
ignored_features = {
    'appDuration',
    'appId',
    'appName',
    'description',
    'Duration',
    'fraction_supported',
    'jobStartTime_min',
    'pluginEnabled',
    'runType',
    'scaleFactor',
    'sparkRuntime',
    'sparkVersion',
    'sqlID'
}

expected_model_features = expected_raw_features - ignored_features


def train(
    cpu_aug_tbl: pd.DataFrame,
    feature_cols: List[str],
    label_col: str,
    n_trials: int = 200,
    base_model: Optional[Booster] = None,
) -> Booster:
    """Train model on preprocessed data."""
    if 'split' not in cpu_aug_tbl.columns:
        raise ValueError(
            'Training data must have a split column with values: train, val, test'
        )

    if LOG_LABEL:
        cpu_aug_tbl = cpu_aug_tbl.copy()
        cpu_aug_tbl[label_col] = np.log(cpu_aug_tbl[label_col])

    # remove nan label entries
    original_num_rows = cpu_aug_tbl.shape[0]
    cpu_aug_tbl = cpu_aug_tbl.loc[~cpu_aug_tbl[label_col].isna()].reset_index(
        drop=True
    )
    if cpu_aug_tbl.shape[0] < original_num_rows:
        logger.warning(
            'Removed %d rows with NaN label values', original_num_rows - cpu_aug_tbl.shape[0]
        )

    # split into train/val/test sets
    x_train = cpu_aug_tbl.loc[cpu_aug_tbl['split'] == 'train', feature_cols]
    y_train = cpu_aug_tbl.loc[cpu_aug_tbl['split'] == 'train', label_col]
    d_train = xgb.DMatrix(x_train, y_train)

    x_val = cpu_aug_tbl.loc[cpu_aug_tbl['split'] == 'val', feature_cols]
    y_val = cpu_aug_tbl.loc[cpu_aug_tbl['split'] == 'val', label_col]
    d_val = xgb.DMatrix(x_val, y_val)

    # dtest should be held out of all train/val steps
    # X_test = cpu_aug_tbl.loc[cpu_aug_tbl['split']=='test', feature_cols]
    # y_test = cpu_aug_tbl.loc[cpu_aug_tbl['split']=='test', label_col]
    # dtest = xgb.DMatrix(X_test, y_test)

    # tune hyperparameters on full dataset
    cpu_aug_tbl = cpu_aug_tbl.sort_values('description').reset_index(drop=True)
    x_tune = cpu_aug_tbl.loc[cpu_aug_tbl['split'] != 'test', feature_cols]
    y_tune = cpu_aug_tbl.loc[cpu_aug_tbl['split'] != 'test', label_col]
    d_tune = xgb.DMatrix(x_tune, y_tune)

    if base_model:
        # use hyperparameters from base model (w/ modifications to learning rate and num trees)
        xgb_params = {}
        cfg = json.loads(base_model.save_config())
        train_params = cfg['learner']['gradient_booster']['tree_train_param']
        xgb_params['eta'] = float(train_params['eta']) / 10.0     # decrease learning rate
        xgb_params['gamma'] = float(train_params['gamma'])
        xgb_params['max_depth'] = int(train_params['max_depth'])
        xgb_params['min_child_weight'] = int(train_params['min_child_weight'])
        xgb_params['subsample'] = float(train_params['subsample'])
        n_estimators = cfg['learner']['gradient_booster']['gbtree_model_param']['num_trees']
        n_estimators = int(float(n_estimators) * 1.1)              # increase n_estimators
    else:
        # use optuna hyper-parameter tuning
        best_params = tune_hyperparameters(x_tune, y_tune, n_trials)
        logger.info(best_params)

        # train model w/ the best hyperparameters using data splits
        base_params = {
            'random_state': 0,
            'objective': 'reg:squarederror',
            'eval_metric': ['mae', 'mape'],  # applied to eval_set/test_data if provided
            'booster': 'gbtree',
        }
        xgb_params = {**base_params, **best_params}
        n_estimators = xgb_params.pop('n_estimators')

    # train model
    evals_result = {}
    xgb_model = xgb.train(
        xgb_params,
        dtrain=d_tune,
        num_boost_round=n_estimators,
        evals=[(d_train, 'train'), (d_val, 'val')],
        verbose_eval=50,
        evals_result=evals_result,
        xgb_model=base_model,
    )
    return xgb_model


def predict(
    xgb_model: xgb.Booster,
    cpu_aug_tbl: pd.DataFrame,
    feature_cols: List[str],
    label_col: str,
) -> pd.DataFrame:
    """Use model to predict on feature data."""
    model_features = xgb_model.feature_names

    missing = set(model_features) - set(feature_cols)
    extra = set(feature_cols) - set(model_features)
    if missing:
        raise ValueError(f'Input is missing model features: {missing}')
    if extra:
        logger.warning('Input had extra features not present in model: %s', extra)

    x = cpu_aug_tbl[model_features]
    y = cpu_aug_tbl[label_col] if label_col else None

    d_mat = xgb.DMatrix(x)
    y_pred = xgb_model.predict(d_mat)

    if LOG_LABEL:
        y_pred = np.exp(y_pred)

    preds = {'y_pred': y_pred}
    if y is not None:
        preds['y'] = y
    preds_df = pd.DataFrame(preds)

    select_columns = [
        'appName',
        'appId',
        'appDuration',
        'sqlID',
        'scaleFactor',
        'Duration',
        'fraction_supported',
        'description',
    ]
    if 'split' in cpu_aug_tbl:
        select_columns.append('split')

    # join predictions with select input features
    results_df = (
        cpu_aug_tbl[select_columns]
        .reset_index(drop=True)
        .merge(preds_df, how='outer', left_index=True, right_index=True)
    )

    if 'y' in results_df.columns:
        # reconstruct original gpu duration for validation purposes
        results_df['gpuDuration'] = results_df['Duration'] / results_df['y']
        results_df['gpuDuration'] = np.floor(results_df['gpuDuration'])

    # adjust raw predictions with stage/sqlID filtering of unsupported ops
    results_df['Duration_pred'] = results_df['Duration'] * (
        1.0
        - results_df['fraction_supported']
        + (results_df['fraction_supported'] / results_df['y_pred'])
    )
    # compute fraction of duration in supported ops
    results_df['Duration_supported'] = (
        results_df['Duration'] * results_df['fraction_supported']
    )
    # compute adjusted speedup (vs. raw speedup prediction: 'y_pred')
    # without qual data, this should be the same as the raw 'y_pred'
    results_df['speedup_pred'] = results_df['Duration'] / results_df['Duration_pred']
    results_df = results_df.drop(columns=['fraction_supported'])

    return results_df


def extract_model_features(
    df: pd.DataFrame, split_functions: Mapping[str, Callable[[pd.DataFrame], pd.DataFrame]] = None
) -> Tuple[pd.DataFrame, List[str], str]:
    """Extract model features from raw features."""
    missing = expected_raw_features - set(df.columns)
    if missing:
        logger.warning('Input dataframe is missing expected raw features: %s', missing)

    if FILTER_SPILLS:
        df = df[
            (df['diskBytesSpilledRatio'] == 0) & (df['memoryBytesSpilled_sum'] == 0)
        ]

    # use CPU runs as primary dataset
    cpu_aug_tbl = df[df['runType'] == 'CPU']

    # remove gpu sql operators from base cpu augmented table.
    # sql_ops_list = [cc for cc in cpu_aug_tbl.columns if cc.startswith('sqlOp_')]
    gpu_sql_ops_list = [
        cc
        for cc in cpu_aug_tbl.columns
        if cc.startswith('sqlOp_Gpu') or 'GpuInsertIntoHadoopFsRelationCommand' in cc
    ]
    cpu_aug_tbl = cpu_aug_tbl.drop(columns=gpu_sql_ops_list)

    gpu_aug_tbl = df[df['runType'] == 'GPU']
    if gpu_aug_tbl.shape[0] > 0:
        if gpu_aug_tbl.shape[0] != cpu_aug_tbl.shape[0]:
            logger.warning(
                'Number of GPU rows (%d) does not match number of CPU rows (%d)',
                gpu_aug_tbl.shape[0],
                cpu_aug_tbl.shape[0],
            )
        # train/validation dataset with CPU + GPU runs
        gpu_aug_tbl = gpu_aug_tbl[
            [
                'appName',
                'scaleFactor',
                'sqlID',
                'Duration',
                'description',
            ]
        ]
        gpu_aug_tbl = gpu_aug_tbl.rename(columns={'Duration': 'xgpu_Duration'})
        cpu_aug_tbl = cpu_aug_tbl.merge(
            gpu_aug_tbl,
            on=['appName', 'scaleFactor', 'sqlID', 'description'],
            how='left',
        )

        # warn for possible mismatched sqlIDs
        num_rows = len(cpu_aug_tbl)
        num_na = cpu_aug_tbl['xgpu_Duration'].isna().sum()
        if (
            num_na / num_rows > 0.05
        ):  # arbitrary threshold, misaligned sqlIDs still may 'match' most of the time
            logger.warning(
                'Percentage of NaN GPU durations is high: %d / %d. Per-sql actual speedups may be inaccurate.',
                num_na,
                num_rows,
            )

        # calculate Duration_speedup
        cpu_aug_tbl['Duration_speedup'] = (
            cpu_aug_tbl['Duration'] / cpu_aug_tbl['xgpu_Duration']
        )
        cpu_aug_tbl = cpu_aug_tbl.drop(columns=['xgpu_Duration'])

        # use Duration_speedup as label
        label_col = 'Duration_speedup'
    else:
        # inference dataset with CPU runs only
        label_col = None

    # add aggregations for time features
    # time_cols = ['executorCPUTime', 'executorDeserializeTime', 'executorDeserializeCPUTime', 'executorRunTime',
    #     'gettingResultTime', 'jvmGCTime', 'resultSerializationTime', 'sr_fetchWaitTime', 'sw_writeTime']
    # time_agg_cols = [cc + '_sum' for cc in time_cols]
    # time_ratio_cols = [cc for cc in cpu_aug_tbl.columns if cc.endswith('TimeRatio')]

    # TODO: investigate leaky_cols
    # leaky_cols = ['duration_min', 'duration_max', 'duration_mean'] + time_agg_cols + time_ratio_cols
    # ignore_cols = ignore_cols  #+ leaky_cols

    # remove non-training columns
    feature_cols = [cc for cc in cpu_aug_tbl.columns if cc not in ignored_features]

    # sanity check features
    actual = set(feature_cols)
    missing = expected_model_features - actual
    extra = actual - expected_model_features
    if missing:
        raise ValueError(f'Input data is missing model features: {missing}')
    if extra:
        # remove extra columns
        logger.warning('Input data has extra features (removed): %s', extra)
        feature_cols = [c for c in feature_cols if c not in extra]

    # add train/val/test split column, if split function(s) provided
    if split_functions:
        # ensure 'split' column in cpu_aug_tbl
        if 'split' not in cpu_aug_tbl.columns:
            cpu_aug_tbl['split'] = pd.Series(dtype='str')

        # save schema, since df.update() defaults all dtypes to floats
        df_schema = cpu_aug_tbl.dtypes

        # extract default split function, if present
        default_split_fn = split_functions.pop('default') if 'default' in split_functions else None

        # handle all other dataset-specific split functions
        for ds_name, split_fn in split_functions.items():
            dataset_df = cpu_aug_tbl.loc[
                (cpu_aug_tbl.appName == ds_name) | (cpu_aug_tbl.appName.str.startswith(f'{ds_name}:'))
            ]
            modified_df = split_fn(dataset_df)
            if modified_df.index.equals(dataset_df.index):
                cpu_aug_tbl.update(modified_df)
                cpu_aug_tbl.astype(df_schema)
            else:
                raise ValueError(f'Plugin: split_function for {ds_name} unexpectedly modified row indices.')
            cpu_aug_tbl.update(dataset_df)

        # handle default split function
        if default_split_fn:
            default_df = cpu_aug_tbl.loc[~cpu_aug_tbl.appName.isin(split_functions.keys())]
            for ds_name in split_functions.keys():
                default_df = default_df.loc[~default_df.appName.str.startswith(f'{ds_name}:')]
            modified_default_df = default_split_fn(default_df)
            if modified_default_df.index.equals(default_df.index):
                cpu_aug_tbl.update(modified_default_df)
                cpu_aug_tbl.astype(df_schema)
            else:
                raise ValueError('Default split_function unexpectedly modified row indices.')
    return cpu_aug_tbl, feature_cols, label_col


def split_random(
    cpu_aug_tbl: pd.DataFrame, seed: int = 0, val_pct: float = 0.2
) -> pd.DataFrame:
    # Random split by percentage
    test_pct = 0.2
    num_rows = len(cpu_aug_tbl)
    indices = list(range(num_rows))
    random.Random(seed).shuffle(indices)

    cpu_aug_tbl.loc[indices[0: int(test_pct * num_rows)], 'split'] = 'test'
    cpu_aug_tbl.loc[
        indices[int(test_pct * num_rows): int((test_pct + val_pct) * num_rows)],
        'split',
    ] = 'val'
    cpu_aug_tbl.loc[indices[int((test_pct + val_pct) * num_rows):], 'split'] = 'train'
    return cpu_aug_tbl


def split_train_val(cpu_aug_tbl: pd.DataFrame, seed: int = 0, val_pct: float = 0.2,) -> pd.DataFrame:
    """Sets all rows without a split value to 'train'."""
    if 'split' in cpu_aug_tbl.columns:
        # if 'split' already present, just modify the NaN rows
        indices = cpu_aug_tbl.index[cpu_aug_tbl['split'].isna()].tolist()
    else:
        # otherwise, modify all rows
        indices = cpu_aug_tbl.index.tolist()

    num_rows = len(indices)
    random.Random(seed).shuffle(indices)

    # Split remaining rows into train/val sets
    cpu_aug_tbl.loc[indices[0: int(val_pct * num_rows)], 'split'] = 'val'
    cpu_aug_tbl.loc[indices[int(val_pct * num_rows):], 'split'] = 'train'
    return cpu_aug_tbl


def split_all_test(cpu_aug_tbl: pd.DataFrame) -> pd.DataFrame:
    """Sets all rows to 'test' split."""
    cpu_aug_tbl['split'] = 'test'
    return cpu_aug_tbl


def tune_hyperparameters(x, y, n_trials: int = 200) -> dict:
    # use full training set for hyperparameter search

    xgb_tmp = xgb.XGBRegressor(objective='reg:squarederror')

    optuna_search_params = {
        'eta': optuna.distributions.FloatDistribution(0.001, 0.05),
        'gamma': optuna.distributions.FloatDistribution(0, 50),
        'max_depth': optuna.distributions.IntDistribution(5, 10),
        'min_child_weight': optuna.distributions.IntDistribution(1, 5),
        'subsample': optuna.distributions.FloatDistribution(
            0.6, 1.0
        ),  # Helps with over fitting
        'n_estimators': optuna.distributions.IntDistribution(50, 100),
    }

    # Optuna with cross-validation:
    optuna_search = optuna.integration.OptunaSearchCV(
        xgb_tmp,
        optuna_search_params,
        cv=5,
        enable_pruning=False,
        scoring='neg_mean_squared_error',
        random_state=0,
        n_trials=n_trials,
        timeout=900,
    )

    # run the search
    optuna_search.fit(x, y)

    return optuna_search.best_params_


def compute_shapley_values(xgb_model: xgb.Booster, features: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Compute Shapley values for the model given an input dataframe.

    Returns two dataframes representing:
    - feature importance
    - raw shapley values (per row of input dataframe)
    """
    pd.set_option('display.max_rows', None)
    explainer = shap.TreeExplainer(xgb_model)
    feature_cols = xgb_model.feature_names
    shap_values = explainer.shap_values(features[feature_cols])

    # raw shap values per row (w/ expected value of model)
    shap_values_df = pd.DataFrame(shap_values, columns=feature_cols)
    shap_values_df['expected_value'] = explainer.expected_value

    # feature importance
    shap_vals = np.abs(shap_values).mean(axis=0)
    feature_importance_df = pd.DataFrame(
        list(zip(feature_cols, shap_vals)), columns=['feature', 'shap_value']
    )
    feature_importance_df = feature_importance_df.sort_values(
        by=['shap_value'], ascending=False
    ).reset_index(drop=True)

    return feature_importance_df, shap_values_df
