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

from typing import Callable, List, Optional
import glob
import json
import os
import numpy as np
import pandas as pd
import traceback
import xgboost as xgb
from importlib_resources import files as package_files
from pathlib import Path
from spark_rapids_tools.tools.qualx.preprocess import (
    load_datasets,
    load_profiles,
    load_qtool_execs,
    load_qual_csv,
    PREPROCESSED_FILE,
    ScanTblError
)
from spark_rapids_tools.tools.qualx.model import (
    extract_model_features,
    compute_feature_importance,
    split_nds,
    split_all_test,
)
from spark_rapids_tools.tools.qualx.model import train as train_model, predict as predict_model
from spark_rapids_tools.tools.qualx.util import (
    compute_accuracy,
    ensure_directory,
    find_paths,
    get_cache_dir,
    get_logger,
    get_dataset_platforms,
    print_summary,
    print_speedup_summary,
    run_profiler_tool,
    run_qualification_tool,
    RegexPattern,
    INTERMEDIATE_DATA_ENABLED
)
from spark_rapids_pytools.common.utilities import Utils
from tabulate import tabulate
from xgboost.core import XGBoostError

logger = get_logger(__name__)


def _get_model(platform: str, model: Optional[str]):
    if not model:
        # try pre-trained model for platform
        model_path = Path(Utils.resource_path(f'qualx/models/xgboost/{platform}.json'))
        if not model_path.exists():
            raise ValueError(
                f'Platform {platform} does not have a pre-trained model, please specify --model or choose another '
                f'platform.'
            )
    else:
        # try pre-trained model first
        model_path = Path(Utils.resource_path(f'qualx/models/xgboost/{platform}.json'))
        if not model_path.exists():
            model_path = model

    logger.info(f'Loading model from: {model_path}')
    xgb_model = xgb.Booster()
    xgb_model.load_model(model_path)
    return xgb_model


def _get_qual_data(qual: Optional[str]):
    if not qual:
        return None, None, None

    # load qual tool execs
    qual_list = find_paths(
        qual, lambda x: RegexPattern.rapidsQualtool.match(x), return_directories=True
    )
    qual_execs = [
        os.path.join(
            q,
            'rapids_4_spark_qualification_output_execs.csv',
        )
        for q in qual_list
    ]
    node_level_supp = load_qtool_execs(qual_execs)

    # load qual tool per-sql predictions
    qual_sql_preds = load_qual_csv(
        qual_list,
        'rapids_4_spark_qualification_output_persql.csv',
        ['App ID', 'SQL ID', 'Estimated GPU Speedup'],
    )

    # load qual tool per-app predictions
    qual_app_preds = load_qual_csv(
        qual_list,
        'rapids_4_spark_qualification_output.csv',
        ['App ID', 'Estimated GPU Speedup'],
    )

    return node_level_supp, qual_app_preds, qual_sql_preds


def _compute_summary(results):
    # summarize speedups per appId
    result_cols = [
        # qualx
        'appName',
        'appId',
        'appDuration',
        'Duration',
        'Duration_pred',
        'Duration_supported',
        # actual
        'gpuDuration',
    ]
    cols = [col for col in result_cols if col in results.columns]
    # compute per-app stats
    summary = (
        results[cols].groupby(['appName', 'appId', 'appDuration']).sum().reset_index()
    )
    if 'gpuDuration' in summary:
        summary['gpuDuration'] = summary['gpuDuration'].replace(0.0, np.nan)

    # compute the fraction of app duration w/ supported ops
    # without qual tool output, this is the entire SQL duration
    # with qual tool output, this is the fraction of SQL w/ supported ops
    summary['fraction_supported'] = (
        summary['Duration_supported'] / summary['appDuration']
    )

    # compute the predicted app duration from original app duration and predicted SQL duration
    # note: this assumes a non-SQL speedup of 1.0
    summary['appDuration_pred'] = (
        summary['appDuration'] - summary['Duration'] + summary['Duration_pred']
    )
    # compute the per-app speedup
    summary['speedup'] = summary['appDuration'] / summary['appDuration_pred']

    # for datasets w/ labels, reconstruct actual speedup per-app
    # TODO: get this from actual CSV file?
    if 'y' in results:
        summary['appDuration_actual'] = (
            summary['appDuration'] - summary['Duration'] + summary['gpuDuration']
        )
        summary['speedup_actual'] = (
            summary['appDuration'] / summary['appDuration_actual']
        )

    # fix dtypes
    long_cols = [
        'appDuration',
        'appDuration_actual',
        'appDuration_pred',
        'Duration',
        'Duration_pred',
        'Duration_supported',
        'gpuDuration',
    ]
    cols = [col for col in long_cols if col in summary.columns]
    summary[cols] = summary[cols].fillna(-1).astype('long')
    return summary


def _predict(
    xgb_model,
    dataset: str,
    input_df: pd.DataFrame,
    *,
    split_fn: Callable[[pd.DataFrame], pd.DataFrame] = None,
    qualtool_filter: Optional[str] = 'stage',
):
    if not input_df.empty:
        filter_str = (
            f'with {qualtool_filter} filtering'
            if any(input_df['fraction_supported'] != 1.0)
            else 'raw'
        )
        logger.info(f'Predicting dataset ({filter_str}): {dataset}')
        features, feature_cols, label_col = extract_model_features(input_df, split_fn)
        # note: dataset name is already stored in the 'appName' field
        try:
            results = predict_model(xgb_model, features, feature_cols, label_col)

            # for evaluation purposes, impute actual GPU values if not provided
            if 'y' not in results:
                results['y'] = np.nan
                results['gpuDuration'] = 0.0

            # compute per-app speedups
            summary = _compute_summary(results)
        except XGBoostError as e:
            # ignore and continue
            logger.error(e)
        except Exception as e:
            # ignore and continue
            logger.error(e)
            traceback.print_exc(e)
    return results, summary


def _read_dataset_scores(
    eval_dir: str, score: str, granularity: str, split: str
) -> pd.DataFrame:
    """Load accuracy scores per dataset.

    Parameters
    ----------
    eval_dir: str
        Path to the output directory of a `qualx evaluate` run.
    score: str
        Type of metric to report: MAPE, wMAPE.
    granularity: str
        Aggregation level for metric: sql, app.
    split: str
        Name of data split to report: train, test, val, all.
        Note: for 'app' granularity, only 'all' is supported.
    """
    mape_csv = glob.glob(f'{eval_dir}/**/*_mape.csv', recursive=True)
    df = pd.concat([pd.read_csv(f) for f in mape_csv])
    df = (
        df.loc[
            (df.score == score) & (df.split == split) & (df.granularity == granularity)
        ]
        .sort_values(['model', 'platform', 'dataset'])
        .reset_index(drop=True)
    )

    nan_df = df[df.isna().any(axis=1)].copy()
    if not nan_df.empty:
        df.dropna(inplace=True)
        nan_df['key'] = (
            nan_df['model'] + '/' + nan_df['platform'] + '/' + nan_df['dataset']
        )
        keys = list(nan_df['key'].unique())
        logger.warn(f'Dropped rows w/ NaN values from: {eval_dir}: {keys}')

    return df


def _read_platform_scores(
    eval_dir: str, score: str, split: str, dataset_filter: List[str] = []
) -> pd.DataFrame:
    """Load accuracy scores per platform.

    Per-app predictions are aggregated by platform to produce the scores.

    Parameters
    ----------
    eval_dir: str
        Path to the output directory of a `qualx evaluate` run.
    score: str
        Type of metric to report: MAPE, wMAPE.
    split: str
        Name of data split to report: train, test, all.
    """
    pred_csv = glob.glob(f'{eval_dir}/**/*_app.csv', recursive=True)

    if split == 'train':
        pred_csv = [f for f in pred_csv if 'test' not in f]
    elif split == 'test':
        pred_csv = [f for f in pred_csv if 'test' in f]

    dfs = []
    for f in pred_csv:
        splits = Path(f).parts
        model = splits[1]
        platform = splits[2] if model == 'combined' else model
        dataset = splits[3] if model == 'combined' else splits[2]
        if dataset_filter and dataset not in dataset_filter:
            continue
        df = pd.read_csv(f)
        df['model'] = model
        df['platform'] = platform
        df['dataset'] = dataset
        dfs.append(df)
    preds_df = pd.concat(dfs)

    nan_df = preds_df[preds_df.isna().any(axis=1)].copy()
    if not nan_df.empty:
        preds_df.dropna(inplace=True)
        nan_df['key'] = (
            nan_df['model'] + '/' + nan_df['platform'] + '/' + nan_df['dataset']
        )
        keys = list(nan_df['key'].unique())
        logger.warn(f'Dropped rows w/ NaN values from: {eval_dir}: {keys}')

    # compute accuracy by platform
    scores = {}
    grouped = preds_df.groupby(['model', 'platform'])
    for (model, platform), group in grouped:
        acc = compute_accuracy(
            group,
            y='Actual speedup',
            y_preds={'Q': 'Q speedup', 'QX': 'QX speedup', 'QXS': 'QXS speedup'},
            weight='appDuration',
        )
        acc_score = {k: v[score] for k, v in acc.items()}
        acc_score['count'] = len(group)
        scores[(model, platform)] = acc_score
    scores_df = pd.DataFrame(scores).transpose().reset_index()
    scores_df.rename(columns={'level_0': 'model', 'level_1': 'platform'}, inplace=True)
    scores_df['count'] = scores_df['count'].astype(int)
    return scores_df, set(preds_df.dataset.unique())


def models():
    """Show available pre-trained models."""
    available_models = [
        model.replace('.json', '')
        for model in os.listdir(package_files('qualx').joinpath('models'))
    ]
    for model in sorted(available_models):
        print(model)
    return


def preprocess(dataset: str):
    """Extract raw features from profiler logs.

    Extract raw features from one or more profiler logs and save the resulting dataframe as a
    parquet file.  This is primarily used to cache preprocessed input data for training.

    Parameters
    ----------
    dataset: str
        Path to a datasets directory for a given platform, e.g. 'datasets/onprem'
    """
    platforms, _ = get_dataset_platforms(dataset)

    # invalidate preprocessed.parquet files
    cache_dir = get_cache_dir()
    for platform in platforms:
        preprocessed_data = f'{cache_dir}/{platform}/{PREPROCESSED_FILE}'
        if os.path.exists(preprocessed_data):
            logger.info(f'Invalidating cached profile_df: {preprocessed_data}')
            os.remove(preprocessed_data)

    load_datasets(dataset, ignore_test=False)
    return


def train(
    dataset: str,
    model: Optional[str] = 'xgb_model.json',
    output_dir: Optional[str] = 'train',
    n_trials: Optional[int] = 200,
):
    """Train an XGBoost model.

    Train a model on the input data specified by either `--preprocessed` or `--dataset`.

    Parameters
    ----------
    dataset:
        Path to a folder containing one or more dataset JSON files.
    model:
        Path to save the trained XGBoost model.
    n_trials:
        Number of trials for hyperparameter search.
    """
    datasets, profile_df = load_datasets(dataset)
    dataset_list = sorted(list(datasets.keys()))
    profile_datasets = sorted(list(profile_df['appName'].unique()))
    logger.info(f'Training on: {dataset_list}')

    # sanity check
    if set(dataset_list) != set(profile_datasets):
        logger.error(
            'Training data contained datasets: {profile_datasets}, expected: {dataset_list}.'
        )

    features, feature_cols, label_col = extract_model_features(profile_df, split_nds)
    xgb_model = train_model(features, feature_cols, label_col, n_trials=n_trials)

    # save model
    ensure_directory(model, parent=True)
    logger.info(f'Saving model to: {model}')
    xgb_model.save_model(model)

    ensure_directory(output_dir)
    compute_feature_importance(xgb_model, features, feature_cols, output_dir)
    return


def predict(
    platform: str,
    output_info: dict,
    *,
    profile: Optional[str] = None,
    qual: Optional[str] = None,
    preprocessed: Optional[str] = None,
    model: Optional[str] = None,
    qualtool_filter: Optional[str] = 'stage',
) -> pd.DataFrame:
    """Predict GPU speedup given CPU logs.

    Predict the speedup of running a Spark application with Spark-RAPIDS on GPUs (vs. CPUs).
    This uses an XGBoost model trained on matching CPU and GPU runs of various Spark applications.

    Note: this provides a 'best guess' based on an ML model, which may show incorrect results when
    compared against actual performance.

    For predictions with filtering of unsupported operators, the input data should be specified by
    the following:
    - `--profile` and `--qual`: predict on existing profiler (and qualification tool) CSV output.
        If `--qual` is provided, this will output predictions with stage filtering of unsupported operators.
        If `--qual` is not provided, this will output the raw predictions from the XGBoost model.

    Advanced usage:
    - `--preprocessed`: return raw predictions and ground truth labels on the output of qualx preprocessing.
        If `--qual` is provided, this will return adjusted predictions with filtering, along with the
        predictions from the qualification tool.  This is primarily used for evaulating models, since
        the preprocessed data includes GPU runs and labels.

    Parameters
    ----------
    platform: str
        Name of platform for spark_rapids_user_tools, e.g. `onprem`, `dataproc`, etc.  This will
        be used as the model name, if --model is not provided.
    model: str
        Either a model name corresponding to a platform/pre-trained model, or the path to an XGBoost
        model on disk.
    profile: str
        Path to a directory containing one or more profiler outputs.
    qual: str
        Path to a directory containing one or more qualtool outputs.
        If supplied, qualtool info about supported/unsupported operators is used to apply modeling to only
        fully supported sqlIDs or heuristically to fully supported stages.
    preprocessed: str
        Path to a directory containing one or more preprocessed datasets in parquet format.
    qualtool_filter: str
        Set to either 'sqlID' or 'stage' (default) to apply model to supported sqlIDs or stages, based on qualtool
        output.  A sqlID or stage is fully supported if all execs are respectively fully supported.
    output_info: dict
        Dictionary containing paths to save predictions as CSV files.
    """
    assert (
        profile or preprocessed
    ), 'One of the following arguments is required: --profile, --preprocessed'

    xgb_model = _get_model(platform, model)
    node_level_supp, _, _ = _get_qual_data(qual)

    # preprocess profiles
    if profile:
        profile_list = find_paths(
            profile,
            lambda x: RegexPattern.rapidsProfile.match(x),
            return_directories=True,
        )
        processed_dfs = {}
        for prof in profile_list:
            datasets = {}
            # add profiles to datasets
            # use parent directory of `rapids_4_spark_profile`
            dataset_name = Path(prof).parent.name
            datasets[dataset_name] = {
                'profiles': [prof],
                'app_meta': {},
                'platform': platform,
            }
            # search profile sub directories for appIds
            appIds = find_paths(
                prof, lambda x: RegexPattern.appId.match(x), return_directories=True
            )
            appIds = [Path(p).name for p in appIds]
            if len(appIds) == 0:
                logger.warn(f'Skipping empty profile: {prof}')
            else:
                try:
                    for appId in appIds:
                        # create dummy app_meta, assuming CPU and scale factor of 1 (for inference)
                        datasets[dataset_name]['app_meta'].update(
                            {appId: {'runType': 'CPU', 'scaleFactor': 1}}
                        )
                    logger.info(f'Loading dataset {dataset_name}')
                    profile_df = load_profiles(
                        datasets, profile, node_level_supp, qualtool_filter
                    )
                    processed_dfs[dataset_name] = profile_df
                except ScanTblError:
                    # ignore
                    logger.error(f'Skipping invalid dataset: {dataset_name}')
    elif preprocessed:
        if os.path.isdir(preprocessed):
            processed = find_paths(preprocessed, lambda x: x.endswith('.parquet'))
        else:
            processed = [preprocessed]
        processed_dfs = {
            dataset.replace('.parquet', ''): pd.read_parquet(dataset)
            for dataset in processed
        }

    if not processed_dfs:
        raise ValueError('No profile data found.')

    # predict on each input dataset
    dataset_summaries = []
    for dataset, input_df in processed_dfs.items():
        dataset_name = Path(dataset).name
        if not input_df.empty:
            filter_str = (
                f'with {qualtool_filter} filtering'
                if node_level_supp is not None
                and any(input_df['fraction_supported'] != 1.0)
                else 'raw'
            )
            logger.info(f'Predicting dataset ({filter_str}): {dataset}')
            features, feature_cols, label_col = extract_model_features(input_df)
            # note: dataset name is already stored in the 'appName' field
            try:
                results = predict_model(xgb_model, features, feature_cols, label_col, output_info)

                # compute per-app speedups
                # _compute_summary takes only one arg
                # summary = _compute_summary(results, qual_preds)
                summary = _compute_summary(results)
                dataset_summaries.append(summary)
                if INTERMEDIATE_DATA_ENABLED:
                    print_summary(summary)

                # compute speedup for the entire dataset
                dataset_speedup = (
                    summary['appDuration'].sum() / summary['appDuration_pred'].sum()
                )
                if INTERMEDIATE_DATA_ENABLED:
                    print(f'Dataset estimated speedup: {dataset_speedup:.2f}')

                # write CSV reports
                sql_predictions_path = output_info['perSql']['path']
                logger.info(f"Writing per-SQL predictions to: {sql_predictions_path}")
                results.to_csv(sql_predictions_path, index=False)

                app_predictions_path = output_info['perApp']['path']
                logger.info(
                    f'Writing per-application predictions to: {app_predictions_path}'
                )
                summary.to_csv(app_predictions_path, index=False)

            except XGBoostError as e:
                # ignore and continue
                logger.error(e)
            except Exception as e:
                # ignore and continue
                logger.error(e)
                traceback.print_exc(e)
        else:
            logger.warn(f'Nothing to predict for dataset {dataset}')

    if dataset_summaries:
        # show summary stats across all datasets
        dataset_summary = pd.concat(dataset_summaries)
        if INTERMEDIATE_DATA_ENABLED:
            print_speedup_summary(dataset_summary)
        return dataset_summary
    return pd.DataFrame()


def evaluate(
    platform: str,
    dataset: str,
    output_dir: str,
    *,
    model: Optional[str] = None,
    qualtool_filter: Optional[str] = 'stage',
):
    """Evaluate model predictions against actual GPU speedup.

    For training datasets with GPU event logs, this returns the actual GPU speedup along with the
    predictions of:
    - Q: qualtool
    - QX: qualx (raw)
    - QXS: qualx (stage filtered)

    Parameters
    ----------
    platform: str
        Name of platform for spark_rapids_user_tools, e.g. `onprem`, `dataproc`, etc.  This will
        be used as the model name, if --model is not provided.
    dataset: str
        Path to a JSON file describing the input dataset.
    output_dir: str
        Path to a directory where the processed files will be saved.
    model: str
        Either a model name corresponding to a platform/pre-trained model, or the path to an XGBoost
        model on disk.
    qualtool_filter: str
        Set to either 'sqlID' or 'stage' (default) to apply model to supported sqlIDs or stages,
        based on qualtool output.  A sqlID or stage is fully supported if all execs are respectively
        fully supported.
    """
    with open(dataset, 'r') as f:
        datasets = json.load(f)
        for ds_name in datasets.keys():
            datasets[ds_name]['platform'] = platform

    splits = os.path.split(dataset)
    dataset_name = splits[-1].replace('.json', '')

    ensure_directory(output_dir)

    xgb_model = _get_model(platform, model)

    cache_dir = get_cache_dir()

    platform_cache = f'{cache_dir}/{platform}'
    profile_dir = f'{platform_cache}/profile'
    qual_dir = f'{platform_cache}/qual'
    ensure_directory(qual_dir)

    quals = os.listdir(qual_dir)
    for ds_name, ds_meta in datasets.items():
        if ds_name not in quals:
            eventlogs = ds_meta['eventlogs']
            for eventlog in eventlogs:
                eventlog = os.path.expandvars(eventlog)
                run_qualification_tool(platform, eventlog, f'{qual_dir}/{ds_name}')

    logger.info('Loading qualification tool CSV files.')
    node_level_supp, qual_app_preds, qual_sql_preds = _get_qual_data(qual_dir)

    logger.info('Loading profiler tool CSV files.')
    profile_df = load_profiles(datasets, profile_dir)  # w/ GPU rows
    filtered_profile_df = load_profiles(
        datasets, profile_dir, node_level_supp, qualtool_filter
    )  # w/o GPU rows
    if profile_df.empty:
        raise ValueError(f'Warning: No profile data found for {dataset}')

    split_fn = split_all_test if 'test' in dataset_name else split_nds

    # raw predictions on unfiltered data
    raw_sql, raw_app = _predict(
        xgb_model,
        dataset_name,
        profile_df,
        split_fn=split_fn,
        qualtool_filter=qualtool_filter,
    )

    # adjusted prediction on filtered data
    filtered_sql, filtered_app = _predict(
        xgb_model,
        dataset_name,
        filtered_profile_df,
        split_fn=split_fn,
        qualtool_filter=qualtool_filter,
    )

    # merge results and join w/ qual_preds
    raw_sql_cols = {
        'appId': 'appId',
        'sqlID': 'sqlID',
        'scaleFactor': 'scaleFactor',
        'appDuration': 'appDuration',
        'Duration': 'Duration',
        'gpuDuration': 'Actual GPU Duration',
        'y': 'Actual speedup',
        'Duration_supported': 'QX Duration_supported',
        'Duration_pred': 'QX Duration_pred',
        'y_pred': 'QX speedup',
        'split': 'split',
    }
    raw_sql = raw_sql.rename(raw_sql_cols, axis=1)
    raw_cols = [col for col in raw_sql_cols.values() if col in raw_sql]

    filtered_sql_cols = {
        'appId': 'appId',
        'appDuration': 'appDuration',
        'sqlID': 'sqlID',
        'scaleFactor': 'scaleFactor',
        'Duration': 'Duration',
        'Duration_supported': 'QXS Duration_supported',
        'Duration_pred': 'QXS Duration_pred',
        # 'y_pred': 'QX speedup',
        'speedup_pred': 'QXS speedup',
    }
    filtered_sql = filtered_sql.rename(filtered_sql_cols, axis=1)

    results_sql = raw_sql[raw_cols].merge(
        filtered_sql[filtered_sql_cols.values()],
        on=['appId', 'sqlID', 'scaleFactor', 'appDuration', 'Duration'],
        how='left',
    )
    results_sql = results_sql.merge(
        qual_sql_preds[['App ID', 'SQL ID', 'Estimated GPU Speedup']],
        left_on=['appId', 'sqlID'],
        right_on=['App ID', 'SQL ID'],
        how='left',
    ).drop_duplicates()
    results_sql = results_sql.drop(columns=['App ID', 'SQL ID'])
    results_sql = results_sql.rename({'Estimated GPU Speedup': 'Q speedup'}, axis=1)

    raw_app_cols = {
        'appId': 'appId',
        'appDuration': 'appDuration',
        'speedup_actual': 'Actual speedup',
        'Duration_pred': 'QX Duration_pred',
        'Duration_supported': 'QX Duration_supported',
        'fraction_supported': 'QX fraction_supported',
        'appDuration_pred': 'QX appDuration_pred',
        'speedup': 'QX speedup',
    }
    raw_app = raw_app.rename(raw_app_cols, axis=1)

    filtered_app_cols = {
        'appId': 'appId',
        'appDuration': 'appDuration',
        'Duration_pred': 'QXS Duration_pred',
        'Duration_supported': 'QXS Duration_supported',
        'fraction_supported': 'QXS fraction_supported',
        'appDuration_pred': 'QXS appDuration_pred',
        'speedup': 'QXS speedup',
    }
    filtered_app = filtered_app.rename(filtered_app_cols, axis=1)

    results_app = raw_app[raw_app_cols.values()].merge(
        filtered_app[filtered_app_cols.values()],
        on=['appId', 'appDuration'],
        how='left',
    )
    results_app = results_app.merge(
        qual_app_preds[['App ID', 'Estimated GPU Speedup']],
        left_on='appId',
        right_on='App ID',
        how='left',
    ).drop_duplicates()
    results_app = results_app.drop(columns=['App ID'])
    results_app = results_app.rename({'Estimated GPU Speedup': 'Q speedup'}, axis=1)

    print(
        '\nComparison of qualx raw (QX), qualx w/ stage filtering (QXS), and qualtool (Q) predictions:'
    )
    print(tabulate(results_app, headers='keys', tablefmt='psql', floatfmt='.2f'))
    print()

    # compute mean abs percentage error (MAPE) for each tool (Q, QX, QXS)

    score_dfs = []
    for granularity, split in [('sql', 'test'), ('sql', 'all'), ('app', 'all')]:
        res = results_app if granularity == 'app' else results_sql
        res = res[res.split == 'test'] if split == 'test' else res
        if res.empty:
            continue

        scores = compute_accuracy(
            res,
            'Actual speedup',
            {'Q': 'Q speedup', 'QX': 'QX speedup', 'QXS': 'QXS speedup'},
            'appDuration' if granularity == 'app' else 'Duration',
        )

        score_df = pd.DataFrame(scores)
        print(f'Scores per {granularity} ({split}):')
        print(tabulate(score_df.transpose(), headers='keys', floatfmt='.4f'))
        print()

        # add identifying columns for these scores
        score_df = score_df.reset_index().rename(columns={'index': 'score'})
        score_df.insert(loc=0, column='model', value=model)
        score_df.insert(loc=1, column='platform', value=platform)
        score_df.insert(loc=2, column='dataset', value=dataset_name)
        score_df.insert(loc=3, column='granularity', value=granularity)
        score_df.insert(loc=4, column='split', value=split)
        score_dfs.append(score_df)

    # write mape scores as CSV
    scores_path = os.path.join(output_dir, f'{dataset_name}_mape.csv')
    ds_scores_df = pd.concat(score_dfs)
    ds_scores_df.to_csv(scores_path, index=False)

    # write results as CSV
    sql_predictions_path = os.path.join(output_dir, f'{dataset_name}_sql.csv')
    logger.info(f'Writing per-SQL predictions to: {sql_predictions_path}')
    results_sql.to_csv(sql_predictions_path, index=False, na_rep='nan')

    app_predictions_path = os.path.join(output_dir, f'{dataset_name}_app.csv')
    logger.info(f'Writing per-application predictions to: {app_predictions_path}')
    results_app.to_csv(app_predictions_path, index=False, na_rep='nan')

    return


def compare(
    previous: str,
    current: str,
    *,
    score: str = 'wMAPE',
    granularity: str = 'sql',
    split: str = 'all',
):
    """Compare evaluation results between versions of code/models.

    This will print the delta of scores and also save them as a CSV file into the `current` directory.

    Parameters
    ----------
    previous: str
        Path to previous evaluation results directory.
    current: str
        Path to current evaluation results directory.
    score: str
        Type of score to compare: 'MAPE' (default) or 'wMAPE'.
    granularity: str
        Granularity of score to compare: 'sql' (default) or 'app'.
    split: str
        Dataset split to compare: 'all' (default), 'train', or 'test'.
    """
    # compare MAPE scores per dataset
    curr_df = _read_dataset_scores(current, score, granularity, split)
    prev_df = _read_dataset_scores(previous, score, granularity, split)

    compare_df = prev_df.merge(
        curr_df,
        how='right',
        on=['model', 'platform', 'dataset', 'granularity', 'split', 'score'],
        suffixes=['_prev', None],
    )
    for score_type in ['Q', 'QX', 'QXS']:
        compare_df[f'{score_type}_delta'] = (
            compare_df[score_type] - compare_df[f'{score_type}_prev']
        )
    print(tabulate(compare_df, headers='keys', tablefmt='psql', floatfmt='.4f'))
    comparison_path = f'{current}/comparison_dataset.csv'
    logger.info(f'Writing dataset evaluation comparison to: {comparison_path}')
    compare_df.to_csv(comparison_path)

    # compare app MAPE scores per platform
    # note: these aggregated scores can be impacted by the addition/removal of datasets
    # for removed datasets, just filter out from 'previous' aggregations
    # for added datasets, warn after printing table for more visibility
    curr_df, curr_datasets = _read_platform_scores(current, score, split)
    prev_df, prev_datasets = _read_platform_scores(
        previous, score, split, curr_datasets
    )

    compare_df = prev_df.merge(
        curr_df,
        how='right',
        on=['model', 'platform'],
        suffixes=['_prev', None],
    )
    for score_type in ['Q', 'QX', 'QXS']:
        compare_df[f'{score_type}_delta'] = (
            compare_df[score_type] - compare_df[f'{score_type}_prev']
        )
    print(tabulate(compare_df, headers='keys', tablefmt='psql', floatfmt='.4f'))
    comparison_path = f'{current}/comparison_platform.csv'
    logger.info(f'Writing platform evaluation comparison to: {comparison_path}')
    compare_df.to_csv(comparison_path)

    # warn user of any new datasets
    added = curr_datasets - prev_datasets
    if added:
        logger.warn(f'New datasets added, comparisons may be skewed: added={added}')
