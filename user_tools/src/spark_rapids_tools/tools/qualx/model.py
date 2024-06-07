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

from typing import Callable, Optional, List, Tuple
import numpy as np
import pandas as pd
import random
import shap
import xgboost as xgb
from spark_rapids_tools.tools.qualx.preprocess import expected_raw_features
from spark_rapids_tools.tools.qualx.util import get_logger, INTERMEDIATE_DATA_ENABLED
from tabulate import tabulate
from xgboost import XGBModel
# Import optional packages
try:
    import optuna
except ImportError:
    optuna = None

logger = get_logger(__name__)

FILTER_SPILLS = False  # remove queries with any disk/mem spill
LOG_LABEL = True  # use log(y) as target


# non-training features (and labels)
ignored_features = set(
    [
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
        'sparkVersion',
        'sqlID',
    ]
)

expected_model_features = expected_raw_features - ignored_features


def train(
    cpu_aug_tbl: pd.DataFrame,
    feature_cols: List[str],
    label_col: str,
    n_trials: int = 200,
) -> XGBModel:
    """Train model on preprocessed data."""
    if 'split' not in cpu_aug_tbl.columns:
        raise ValueError(
            'Training data must have a split column with values: train, val, test'
        )

    if LOG_LABEL:
        cpu_aug_tbl = cpu_aug_tbl.copy()
        cpu_aug_tbl[label_col] = np.log(cpu_aug_tbl[label_col])

    # split into train/val/test sets
    X_train = cpu_aug_tbl.loc[cpu_aug_tbl['split'] == 'train', feature_cols]
    y_train = cpu_aug_tbl.loc[cpu_aug_tbl['split'] == 'train', label_col]
    dtrain = xgb.DMatrix(X_train, y_train)

    X_val = cpu_aug_tbl.loc[cpu_aug_tbl['split'] == 'val', feature_cols]
    y_val = cpu_aug_tbl.loc[cpu_aug_tbl['split'] == 'val', label_col]
    dval = xgb.DMatrix(X_val, y_val)

    # dtest should be held out of all train/val steps
    # X_test = cpu_aug_tbl.loc[cpu_aug_tbl['split']=='test', feature_cols]
    # y_test = cpu_aug_tbl.loc[cpu_aug_tbl['split']=='test', label_col]
    # dtest = xgb.DMatrix(X_test, y_test)

    # tune hyperparamters on full dataset
    cpu_aug_tbl = cpu_aug_tbl.sort_values('description').reset_index(drop=True)
    X_tune = cpu_aug_tbl.loc[cpu_aug_tbl['split'] != 'test', feature_cols]
    y_tune = cpu_aug_tbl.loc[cpu_aug_tbl['split'] != 'test', label_col]
    dtune = xgb.DMatrix(X_tune, y_tune)

    best_params = tune_hyperparameters(X_tune, y_tune, n_trials)
    logger.info(best_params)

    # train model w/ best hyperparameters using data splits
    base_params = {
        'random_state': 0,
        'objective': 'reg:squarederror',
        'eval_metric': ['mae', 'mape'],  # applied to eval_set/test_data if provided
        'booster': 'gbtree',
    }
    xgb_params = {**base_params, **best_params}
    xgb_params.pop('n_estimators')

    # train model
    evals_result = {}
    xgb_model = xgb.train(
        xgb_params,
        dtrain=dtune,
        num_boost_round=best_params['n_estimators'],
        evals=[(dtrain, 'train'), (dval, 'val')],
        verbose_eval=50,
        evals_result=evals_result,
    )
    return xgb_model


def predict(
    xgb_model: xgb.Booster,
    cpu_aug_tbl: pd.DataFrame,
    feature_cols: List[str],
    label_col: str,
    output_info: Optional[dict] = None,
) -> pd.DataFrame:
    """Use model to predict on feature data."""
    model_features = xgb_model.feature_names

    missing = set(model_features) - set(feature_cols)
    extra = set(feature_cols) - set(model_features)
    if missing:
        raise ValueError(f'Input is missing model features: {missing}')
    if extra:
        logger.warn(f'Input had extra features not present in model: {extra}')

    X = cpu_aug_tbl[model_features]
    y = cpu_aug_tbl[label_col] if label_col else None

    dmat = xgb.DMatrix(X)
    y_pred = xgb_model.predict(dmat)

    # compute SHAPley values for the model
    if output_info:
        shap_values_output_file = output_info['shapValues']['path']
        compute_shapley_values(xgb_model, X, feature_cols, shap_values_output_file)

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
        results_df["gpuDuration"] = np.floor(
            results_df["gpuDuration"]
        )  # .astype("long")

    # adjust raw predictions with stage/sqlID filtering of unsupporteds
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
    df: pd.DataFrame, split_fn: Callable[[pd.DataFrame], pd.DataFrame] = None
) -> Tuple[pd.DataFrame, List[str], str]:
    """Extract model features from raw features."""
    missing = expected_raw_features - set(df.columns)
    if missing:
        logger.warn(f'Input dataframe is missing expected raw features: {missing}')

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
            logger.warn(
                'Number of GPU rows ({}) does not match number of CPU rows ({})'.format(
                    gpu_aug_tbl.shape[0], cpu_aug_tbl.shape[0]
                )
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
        gpu_aug_tbl = gpu_aug_tbl.rename(columns={"Duration": "xgpu_Duration"})
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
            logger.warn(
                f'Percentage of NaN GPU durations is high: {num_na} / {num_rows}  Per-sql actual speedups may be inaccurate.'
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
    # time_cols = ['executorCPUTime', 'executorDeserializeTime', 'executorDeserializeCPUTime', 'executorRunTime', 'gettingResultTime',
    #     'jvmGCTime', 'resultSerializationTime', 'sr_fetchWaitTime', 'sw_writeTime']
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
        logger.warn(f'Input data has extra features (removed): {extra}')
        feature_cols = [c for c in feature_cols if c not in extra]

    # add train/val/test split column, if split function provided
    if split_fn:
        cpu_aug_tbl = split_fn(cpu_aug_tbl)

    return cpu_aug_tbl, feature_cols, label_col


def compute_feature_importance(xgb_model, features, feature_cols, output_dir):
    pd.set_option('display.max_rows', None)

    # feature importance
    print('XGBoost feature importance:')
    feature_importance = xgb_model.get_score(importance_type='gain')
    importance_df = (
        pd.DataFrame(feature_importance, index=[0]).transpose().reset_index()
    )
    importance_df.columns = ['feature', 'importance']
    importance_df = importance_df.sort_values(
        'importance', ascending=False
    ).reset_index(drop=True)
    importance_df.to_csv(f'{output_dir}/feature_importance.csv')
    print(tabulate(importance_df, headers='keys', tablefmt='psql', floatfmt='.2f'))

    # shapley values for train/test data
    explainer = shap.TreeExplainer(xgb_model)
    for split in ['train', 'test']:
        features_split = features[feature_cols].loc[features['split'] == split]
        if features_split.empty:
            continue

        shap_values = explainer.shap_values(features_split)
        shap_vals = np.abs(shap_values).mean(axis=0)
        shap_df = pd.DataFrame(
            list(zip(feature_cols, shap_vals)), columns=['feature', 'shap_value']
        )
        shap_df = shap_df.sort_values(by=['shap_value'], ascending=False).reset_index(
            drop=True
        )
        shap_df.to_csv(f'{output_dir}/shap_{split}.csv')

        print(f'Shapley values ({split}):')
        print(tabulate(shap_df, headers='keys', tablefmt='psql', floatfmt='.2f'))


def split_random(
    cpu_aug_tbl: pd.DataFrame, seed: int = 0, val_pct: float = 0.2
) -> pd.DataFrame:
    # Random split by percentage
    test_pct = 0.2
    num_rows = len(cpu_aug_tbl)
    indices = list(range(num_rows))
    random.Random(seed).shuffle(indices)

    cpu_aug_tbl.loc[indices[0 : int(test_pct * num_rows)], 'split'] = 'test'
    cpu_aug_tbl.loc[
        indices[int(test_pct * num_rows) : int((test_pct + val_pct) * num_rows)],
        'split',
    ] = 'val'
    cpu_aug_tbl.loc[indices[int((test_pct + val_pct) * num_rows) :], 'split'] = 'train'
    return cpu_aug_tbl


def split_all_test(cpu_aug_tbl: pd.DataFrame) -> pd.DataFrame:
    cpu_aug_tbl['split'] = 'test'
    return cpu_aug_tbl


def split_nds(
    cpu_aug_tbl: pd.DataFrame,
    seed: int = 0,
    val_pct: float = 0.2,
    holdout_queries: List[str] = [
        'query2',
        'query24_part1',
        'query24_part2',
        'query28',
        'query47',
        'query56',
        'query66',
        'query75',
        'query83',
        'query93',
        'query99',
    ],
) -> pd.DataFrame:
    is_test = (cpu_aug_tbl['appName'].str.startswith('nds')) & (
        cpu_aug_tbl['description'].isin(holdout_queries)
    )
    cpu_aug_tbl.loc[is_test, 'split'] = 'test'
    indices = cpu_aug_tbl.index[~is_test].tolist()
    num_rows = len(indices)
    random.Random(seed).shuffle(indices)

    # Split remaining rows into train/val sets
    cpu_aug_tbl.loc[indices[0 : int(val_pct * num_rows)], 'split'] = 'val'
    cpu_aug_tbl.loc[indices[int(val_pct * num_rows) :], 'split'] = 'train'
    return cpu_aug_tbl


def tune_hyperparameters(X, y, n_trials: int = 200) -> dict:
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
    optuna_search.fit(X, y)

    return optuna_search.best_params_


def compute_shapley_values(
        xgb_model: xgb.Booster,
        x_dim: pd.DataFrame,
        feature_cols: List[str],
        output_file: str):
    """
    Compute SHAPley values for the model.
    """
    pd.set_option('display.max_rows', None)
    explainer = shap.TreeExplainer(xgb_model)
    shap_values = explainer.shap_values(x_dim)
    shap_vals = np.abs(shap_values).mean(axis=0)
    feature_importance = pd.DataFrame(
        list(zip(feature_cols, shap_vals)), columns=['feature', 'shap_value']
    )
    feature_importance.sort_values(by=['shap_value'], ascending=False, inplace=True)
    logger.info('Writing SHAPley values to: %s', output_file)
    feature_importance.to_csv(output_file, index=False)
    if INTERMEDIATE_DATA_ENABLED:
        logger.info('Feature importance (SHAPley values)\n %s', feature_importance)
