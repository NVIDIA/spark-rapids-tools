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
import fire
import glob
import json
import os
import pandas as pd
import traceback
import xgboost as xgb
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
    run_qualification_tool,
    RegexPattern,
    INTERMEDIATE_DATA_ENABLED, create_row_with_default_speedup, write_csv_reports
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
        model_path = Path(Utils.resource_path(f'qualx/models/xgboost/{model}.json'))
        if not model_path.exists():
            model_path = model

    logger.info(f'Loading model from: {model_path}')
    xgb_model = xgb.Booster()
    xgb_model.load_model(model_path)
    return xgb_model


def _get_qual_data(qual: Optional[str]):
    if not qual:
        return None, None, None, None

    # load qual tool execs
    qual_list = find_paths(
        qual, lambda x: RegexPattern.rapidsQualtool.match(x), return_directories=True
    )
    # load metrics directory from all qualification paths.
    # metrics follow the pattern 'qual_2024xx/rapids_4_spark_qualification_output/raw_metrics'
    qual_metrics = [
        path
        for q in qual_list
        for path in find_paths(q, RegexPattern.qualToolMetrics.match, return_directories=True)
    ]
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
    qualtool_output = load_qual_csv(
        qual_list,
        'rapids_4_spark_qualification_output.csv',
        ['App Name', 'App ID', 'App Duration', 'Estimated GPU Speedup'],
    )

    return node_level_supp, qualtool_output, qual_sql_preds, qual_metrics


def _compute_summary(results):
    # summarize speedups per appId
    result_cols = [
        # qualx
        'appName',
        'appId',
        'appDuration',
        'description',
        'Duration',
        'Duration_pred',
        'Duration_supported',
        'scaleFactor',
    ]
    cols = [col for col in result_cols if col in results.columns]
    # compute per-app stats
    group_by_cols = ['appName', 'appId', 'appDuration', 'scaleFactor']
    summary = (
        results[cols]
        .groupby(group_by_cols)
        .agg(
            {
                'Duration': 'sum',
                'Duration_pred': 'sum',
                'Duration_supported': 'sum',
                'description': 'first',
            }
        )
        .reset_index()
    )

    # compute the fraction of app duration w/ supported ops
    # without qual tool output, this is the entire SQL duration
    # with qual tool output, this is the fraction of SQL w/ supported ops
    summary['appDuration'] = summary['appDuration'].clip(lower=summary['Duration'])
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

            # compute per-app speedups
            summary = _compute_summary(results)

            if 'y' in results:
                results = results.loc[~results.y.isna()].reset_index(drop=True)
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
        Type of metric to report.
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
        Type of metric to report
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


def _add_entries_for_missing_apps(all_default_preds: pd.DataFrame, summary_df: pd.DataFrame) -> pd.DataFrame:
    """
    Add entry with default speedup prediction for apps that are missing in the summary.
    """
    # identify apps that do not have predictions in the summary
    missing_apps = all_default_preds[~all_default_preds['appId'].isin(summary_df['appId'])].copy()
    missing_apps['wasPredicted'] = False
    summary_df['wasPredicted'] = True
    # add missing applications with default predictions to the summary
    return pd.concat([summary_df, missing_apps], ignore_index=True)


def models():
    """Show available pre-trained models."""
    available_models = [
        model.replace('.json', '')
        for model in os.listdir(
            Path(Utils.resource_path('qualx/models/xgboost'))
        )
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
    qual: str,
    output_info: dict,
    *,
    model: Optional[str] = None,
    qualtool_filter: Optional[str] = 'stage',
) -> pd.DataFrame:
    xgb_model = _get_model(platform, model)
    node_level_supp, qualtool_output, _, qual_metrics = _get_qual_data(qual)
    # create a DataFrame with default predictions for all app IDs.
    # this will be used for apps without predictions.
    default_preds_df = qualtool_output.apply(create_row_with_default_speedup, axis=1)
    # add a column for dataset names to associate apps with datasets.
    default_preds_df['dataset_name'] = None

    # if qualification metrics are provided, load metrics and apply filtering
    processed_dfs = {}
    if len(qual_metrics) > 0:
        for metrics_dir in qual_metrics:
            datasets = {}
            # add metrics directory to datasets
            # metrics follow the pattern 'qual_2024xx/rapids_4_spark_qualification_output/raw_metrics'
            # use grandparent directory as dataset name 'qual_2024xxxx'
            dataset_name = Path(metrics_dir).parent.parent.name
            datasets[dataset_name] = {
                'profiles': [metrics_dir],
                'app_meta': {},
                'platform': platform,
            }
            # search sub directories for appIds
            appIds = find_paths(
                metrics_dir, lambda x: RegexPattern.appId.match(x), return_directories=True
            )
            appIds = [Path(p).name for p in appIds]
            if len(appIds) == 0:
                logger.warn(f'Skipping empty metrics directory: {metrics_dir}')
            else:
                try:
                    for appId in appIds:
                        # create dummy app_meta, assuming CPU and scale factor of 1 (for inference)
                        datasets[dataset_name]['app_meta'].update(
                            {appId: {'runType': 'CPU', 'scaleFactor': 1}}
                        )
                        # update the dataset_name for each appId
                        default_preds_df.loc[default_preds_df['appId'] == appId, 'dataset_name'] = dataset_name
                    logger.info(f'Loading dataset {dataset_name}')
                    metrics_df = load_profiles(
                        datasets=datasets,
                        node_level_supp=node_level_supp,
                        qualtool_filter=qualtool_filter,
                        qualtool_output=qualtool_output
                    )
                    processed_dfs[dataset_name] = metrics_df
                except ScanTblError:
                    # ignore
                    logger.error(f'Skipping invalid dataset: {dataset_name}')
    else:
        logger.warning('Qualification tool metrics are missing. Speedup predictions will be skipped.')
        return pd.DataFrame()

    if not processed_dfs:
        # this is an error condition, and we should not fall back to the default predictions.
        raise ValueError('Data preprocessing resulted in an empty dataset. Speedup predictions will be skipped.')

    # predict on each input dataset
    dataset_summaries = []
    for dataset, input_df in processed_dfs.items():
        dataset_name = Path(dataset).name
        # filter default predictions for the current dataset and drop the dataset_name column
        filtered_default_preds = default_preds_df[default_preds_df['dataset_name'] == dataset_name].copy()
        filtered_default_preds.drop(columns=['dataset_name'], inplace=True)
        # use default predictions if no input data is available
        per_app_summary = filtered_default_preds
        per_sql_summary = None
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
                per_sql_summary = predict_model(xgb_model, features, feature_cols, label_col, output_info)
                # compute per-app speedups
                # _compute_summary takes only one arg
                # summary = _compute_summary(results, qual_preds)
                summary = _compute_summary(per_sql_summary)
                # combine calculated summary with default predictions for missing apps
                per_app_summary = _add_entries_for_missing_apps(filtered_default_preds, summary)
                if INTERMEDIATE_DATA_ENABLED:
                    print_summary(per_app_summary)

                # compute speedup for the entire dataset
                dataset_speedup = (
                    per_app_summary['appDuration'].sum() / per_app_summary['appDuration_pred'].sum()
                )
                if INTERMEDIATE_DATA_ENABLED:
                    print(f'Dataset estimated speedup: {dataset_speedup:.2f}')
            except XGBoostError as e:
                # ignore and continue
                logger.error(e)
            except Exception as e:
                # ignore and continue
                logger.error(e)
                traceback.print_exc(e)
        else:
            logger.warn(f'Predicted speedup will be 1.0 for dataset: {dataset}. Check logs for details.')
        # TODO: Writing CSV reports for all datasets to the same location. We should write to separate directories.
        write_csv_reports(per_sql_summary, per_app_summary, output_info)
        dataset_summaries.append(per_app_summary)

    if dataset_summaries:
        # show summary stats across all datasets
        dataset_summary = pd.concat(dataset_summaries)
        if INTERMEDIATE_DATA_ENABLED:
            print_speedup_summary(dataset_summary)
        return dataset_summary
    return pd.DataFrame()


def __predict_cli(
    platform: str,
    eventlogs: str,
    output_dir: str,
    *,
    model: Optional[str] = None,
    qualtool_filter: Optional[str] = 'stage',
):
    """Predict GPU speedup given CPU logs.

    Predict the speedup of running a Spark application with Spark-RAPIDS on GPUs (vs. CPUs).
    This uses an XGBoost model trained on matching CPU and GPU runs of various Spark applications.

    Note: this provides a 'best guess' based on an ML model, which may show incorrect results when
    compared against actual performance.

    Parameters
    ----------
    platform: str
        Name of platform for spark_rapids_user_tools, e.g. `onprem`, `dataproc`, etc.  This will
        be used as the model name, if --model is not provided.
    eventlogs: str
        Path to a single event log, or a directory containing multiple event logs.
    output_dir:
        Path to save predictions as CSV files.
    model: str
        Either a model name corresponding to a platform/pre-trained model, or the path to an XGBoost
        model on disk.
    qualtool_filter: str
        Set to either 'sqlID' or 'stage' (default) to apply model to supported sqlIDs or stages, based on qualtool
        output.  A sqlID or stage is fully supported if all execs are respectively fully supported.
    """
    # Note this function is for internal usage only. `spark_rapids predict` cmd is the public interface.

    # Construct output paths
    ensure_directory(output_dir)

    if not any([f.startswith('qual_') for f in os.listdir(output_dir)]):
        # run qual tool if no existing qual output found
        run_qualification_tool(platform, eventlogs, output_dir)
    qual = output_dir

    output_info = {
        'perSql': {'path': os.path.join(output_dir, 'per_sql.csv')},
        'perApp': {'path': os.path.join(output_dir, 'per_app.csv')},
        'shapValues': {'path': os.path.join(output_dir, 'shap_values.csv')},
    }

    predict(
        platform,
        output_info=output_info,
        qual=qual,
        model=model,
        qualtool_filter=qualtool_filter,
    )


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
    node_level_supp, qualtool_output, qual_sql_preds, _ = _get_qual_data(qual_dir)

    logger.info('Loading profiler tool CSV files.')
    profile_df = load_profiles(datasets, profile_dir)  # w/ GPU rows
    filtered_profile_df = load_profiles(
        datasets, profile_dir, node_level_supp, qualtool_filter, qualtool_output
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
    # join raw app data with app level gpu ground truth
    app_durations = (
        profile_df.loc[profile_df.runType == 'GPU'][
            ['appName', 'appDuration', 'description', 'scaleFactor']
        ]
        .groupby(['appName', 'appDuration', 'scaleFactor'])
        .first()
        .reset_index()
    )
    app_durations = app_durations.rename(columns={'appDuration': 'gpu_appDuration'})

    # handle query per app and regular app differently.
    # For the former, we need to use query description field to join cpu and gpu data
    # since appname is the same for all queries/apps in these case.

    raw_app_regular = raw_app.loc[~raw_app.appName.str.contains('query_per_app')]
    raw_app_q_per_app = raw_app.loc[raw_app.appName.str.contains('query_per_app')]
    app_durations_regular = app_durations.loc[
        ~app_durations.appName.str.contains('query_per_app')
    ][['appName', 'gpu_appDuration', 'scaleFactor']]
    app_durations_q_per_app = app_durations.loc[
        app_durations.appName.str.contains('query_per_app')
    ]

    raw_app_regular = raw_app_regular.merge(
        app_durations_regular[['appName', 'gpu_appDuration', 'scaleFactor']],
        on=['appName', 'scaleFactor'],
        how='left',
    )
    raw_app_q_per_app = raw_app_q_per_app.merge(
        app_durations_q_per_app, on=['appName', 'description', 'scaleFactor'], how='left'
    )

    raw_app = pd.concat([raw_app_regular, raw_app_q_per_app])

    if not raw_app.loc[raw_app.gpu_appDuration.isna()].empty:
        logger.error(
            f'missing gpu apps: {raw_app.loc[raw_app.gpu_appDuration.isna()].to_markdown()}'
        )
        raw_app = raw_app.loc[~raw_app.gpu_appDuration.isna()]

    raw_app = raw_app.rename({'gpu_appDuration': 'appDuration_actual'}, axis=1)
    raw_app['speedup_actual'] = raw_app['appDuration'] / raw_app['appDuration_actual']

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
        qualtool_output[['App ID', 'Estimated GPU Speedup']],
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
    score: str = 'dMAPE',
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
        Type of score to compare: 'MAPE', 'wMAPE', or 'dMAPE' (default).
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


def entrypoint():
    """
    These are commands are intended for internal usage only.
    """
    cmds = {
        "models": models,
        "preprocess": preprocess,
        "train": train,
        "predict": __predict_cli,
        "evaluate": evaluate,
        "compare": compare,
    }
    fire.Fire(cmds)


if __name__ == "__main__":
    entrypoint()
