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

""" Main module for QualX related commands """

from typing import Callable, List, Optional, Tuple, Set
import glob
import json
import os
import traceback
from pathlib import Path
from tabulate import tabulate
from xgboost.core import XGBoostError, Booster
import numpy as np
import pandas as pd
import xgboost as xgb
import fire

from spark_rapids_tools import CspPath
from spark_rapids_tools.tools.qualx.preprocess import (
    load_datasets,
    load_profiles,
    load_qtool_execs,
    load_qual_csv,
    PREPROCESSED_FILE
)
from spark_rapids_tools.tools.qualx.model import (
    extract_model_features,
    compute_shapley_values,
    split_all_test,
    split_train_val,
)
from spark_rapids_tools.tools.qualx.model import train as train_model, predict as predict_model
from spark_rapids_tools.tools.qualx.util import (
    compute_accuracy,
    ensure_directory,
    find_paths,
    get_cache_dir,
    get_logger,
    get_dataset_platforms,
    load_plugin,
    print_summary,
    print_speedup_summary,
    run_qualification_tool,
    RegexPattern,
    INTERMEDIATE_DATA_ENABLED, create_row_with_default_speedup, write_csv_reports
)
from spark_rapids_pytools.common.utilities import Utils


logger = get_logger(__name__)


def _get_model_path(platform: str, model: Optional[str], variant: Optional[str] = None) -> Path:
    if model is not None:
        # if "model" is provided
        if CspPath.is_file_path(model, raise_on_error=False):
            # and "model" is actually represents a file path
            # check that it is valid json file
            if not CspPath.is_file_path(model,
                                        extensions=['json'],
                                        raise_on_error=False):
                raise ValueError(
                    f'Custom model file [{model}] is invalid. Please specify a valid JSON file.')
            # TODO: If the path is remote, we need to copy it locally in order to successfully
            #       load it with xgboost.
            model_path = Path(CspPath(model).no_scheme)
            if not model_path.exists():
                raise FileNotFoundError(f'Model JSON file not found: {model_path}')
        else:
            # otherwise, try loading pre-trained model by "short name", e.g. "onprem"
            model_path = Path(Utils.resource_path(f'qualx/models/xgboost/{model}.json'))
            if not model_path.exists():
                raise FileNotFoundError(f'Model JSON file for {model} not found at: {model_path}')
    else:
        # if "model" not provided, try loading pre-trained model by "platform" (and "variant")
        variant_suffixes = {
            'SPARK': '',
            'SPARK_RAPIDS': '',
            'PHOTON': '_photon'
        }
        variant_suffix = variant_suffixes.get(variant, '')
        model_path = Path(Utils.resource_path(f'qualx/models/xgboost/{platform}{variant_suffix}.json'))
        if not model_path.exists():
            if not variant_suffix:
                raise ValueError(
                    f'Platform [{platform}] does not have a pre-trained model, '
                    'please specify --model or choose another platform.'
                )
            # variant model not found, try base platform model
            model_path = Path(Utils.resource_path(f'qualx/models/xgboost/{platform}.json'))
            if model_path.exists():
                logger.warning(
                    'Platform [%s%s] does not have a pretrained model, using [%s] model',
                    platform,
                    variant_suffix,
                    platform,
                )
            else:
                raise ValueError(
                    f'Platform [{platform}{variant_suffix}] does not have a pre-trained model, '
                    'please specify --model or choose another platform.'
                )
    return model_path


def _get_model(platform: str,
               model: Optional[str] = None,
               variant: Optional[str] = None) -> Booster:
    """
    Load the XGBoost model from a specified path or use the pre-trained model for the platform.

    The "model" argument has a precedence over the other input options. If it is defined, this function checks
    if it is a valid path to an XGBoost model or a string literal matching a pre-trained model name.
    If "model" is not defined, then the pre-trained model matching "platform" will be used.

    Parameters
    ----------
    platform: str
        Name of the platform used to define the path of the pre-trained model.
        The platform should match a JSON file in the "resources" directory.
    model: str
        Either a path to a model file or the name of a pre-trained model.
        If the input is a string literal that cannot be a file path, then it is assumed that the file
        is located under the resources directory.
    variant: str
        Value of the `sparkRuntime` column from the `application_information.csv` file from the profiler tool.
        If set, will be used to load a pre-trained model matching the platform and variant.

    Returns
    -------
    xgb.Booster model file.
    """
    model_path = _get_model_path(platform, model, variant)
    logger.info('Loading model from: %s', model_path)
    xgb_model = xgb.Booster()
    xgb_model.load_model(model_path)
    return xgb_model


def _get_qual_data(qual: Optional[str]) -> Tuple[
    Optional[pd.DataFrame],
    Optional[pd.DataFrame],
    List[str]
]:
    if not qual:
        return None, None, []

    # load qual tool execs
    qual_list = find_paths(
        qual, RegexPattern.rapids_qual.match, return_directories=True
    )
    # load metrics directory from all qualification paths.
    # metrics follow the pattern 'qual_2024xx/rapids_4_spark_qualification_output/raw_metrics'
    qual_metrics = [
        path
        for q in qual_list
        for path in find_paths(q, RegexPattern.qual_tool_metrics.match, return_directories=True)
    ]
    qual_execs = [
        os.path.join(
            q,
            'rapids_4_spark_qualification_output_execs.csv',
        )
        for q in qual_list
    ]
    node_level_supp = load_qtool_execs(qual_execs)

    # load qual tool per-app predictions
    qualtool_output = load_qual_csv(
        qual_list,
        'rapids_4_spark_qualification_output.csv',
        ['App Name', 'App ID', 'App Duration'],
    )

    return node_level_supp, qualtool_output, qual_metrics


def _compute_summary(results: pd.DataFrame) -> pd.DataFrame:
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
    summary: pd.DataFrame = (
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
    qual_tool_filter: Optional[str] = 'stage',
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    results = pd.DataFrame(
        columns=[
            'appId',
            'appDuration',
            'sqlID',
            'scaleFactor',
            'Duration',
            'Duration_supported',
            'Duration_pred',
            'speedup_pred',
        ]
    )
    summary = pd.DataFrame(
        columns=[
            'appId',
            'appDuration',
            'Duration_pred',
            'Duration_supported',
            'fraction_supported',
            'appDuration_pred',
            'speedup',
        ]
    )
    if not input_df.empty:
        filter_str = (
            f'with {qual_tool_filter} filtering'
            if any(input_df['fraction_supported'] != 1.0)
            else 'raw'
        )
        logger.info('Predicting dataset (%s): %s', filter_str, dataset)
        features, feature_cols, label_col = extract_model_features(input_df, {'default': split_fn})
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
        except Exception as e:  # pylint: disable=broad-except
            # ignore and continue
            logger.error(e)
            traceback.print_exc()
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
        logger.warning('Dropped rows w/ NaN values from: %s: %s', eval_dir, keys)

    return df


def _read_platform_scores(
    eval_dir: str, score: str, split: str, dataset_filter: List[str] = None
) -> Tuple[pd.DataFrame, Set[str]]:
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
        logger.warning('Dropped rows w/ NaN values from: %s: %s', eval_dir, keys)

    # compute accuracy by platform
    scores = {}
    grouped = preds_df.groupby(['model', 'platform'])
    for (model, platform), group in grouped:
        acc = compute_accuracy(
            group,
            y='Actual speedup',
            y_preds={'QX': 'QX speedup', 'QXS': 'QXS speedup'},
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


def models() -> None:
    """Show available pre-trained models."""
    available_models = [
        model.replace('.json', '')
        for model in os.listdir(
            Path(Utils.resource_path('qualx/models/xgboost'))
        )
    ]
    for model in sorted(available_models):
        print(model)


def preprocess(dataset: str) -> None:
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
            logger.info('Invalidating cached profile_df: %s', preprocessed_data)
            os.remove(preprocessed_data)

    load_datasets(dataset, ignore_test=False)


def train(
    dataset: str,
    model: Optional[str] = 'xgb_model.json',
    output_dir: Optional[str] = 'train',
    n_trials: Optional[int] = 200,
    base_model: Optional[str] = None,
    features_csv_dir: Optional[str] = None,
) -> None:
    """Train an XGBoost model.

    Train a model on the input data specified by either `--preprocessed` or `--dataset`.

    Parameters
    ----------
    dataset:
        Path to a folder containing one or more dataset JSON files.
    model:
        Path to save the trained XGBoost model.
    output_dir:
        Path to save the output files, e.g. SHAP values, feature importance.
    n_trials:
        Number of trials for hyperparameter search.
    base_model:
        Path to an existing pre-trained model to continue training from.
    features_csv_dir:
        Path to a directory containing one or more features.csv files.  These files are produced during prediction,
        and must be manually edited to provide a label column (Duration_speedup) and value.
    """
    datasets, profile_df = load_datasets(dataset)
    dataset_list = sorted(list(datasets.keys()))
    profile_datasets = sorted(list(profile_df['appName'].unique()))
    logger.info('Training on: %s', dataset_list)

    # sanity check
    if set(dataset_list) != set(profile_datasets):
        logger.warning(
            'Training data contained datasets: %s, expected: %s', profile_datasets, dataset_list
        )

    split_functions = {'default': split_train_val}
    for ds_name, ds_meta in datasets.items():
        if 'split_function' in ds_meta:
            plugin_path = ds_meta['split_function']
            logger.info('Using split function for %s dataset from plugin: %s', ds_name, plugin_path)
            plugin = load_plugin(plugin_path)
            split_functions[ds_name] = plugin.split_function

    features, feature_cols, label_col = extract_model_features(profile_df, split_functions)

    if features_csv_dir:
        if not Path(features_csv_dir).exists():
            raise FileNotFoundError(f'Features directory not found: {features_csv_dir}')
        features_csv_files = glob.glob(f'{features_csv_dir}/**/*.csv', recursive=True)
        df = pd.concat([pd.read_csv(f) for f in features_csv_files])
        if df[label_col].isnull().any():
            raise ValueError(
                'Additional features contained an empty/null label, '
                f'please add a label column ({label_col}) and value.'
            )
        df['split'] = 'train'
        logger.info(
            'Concatenating %s row(s) of additional features to training set from: %s', len(df), features_csv_dir
        )
        features = pd.concat([features, df])

    xgb_base_model = None
    if base_model:
        if os.path.exists(base_model):
            logger.info('Fine-tuning on base model from: %s', base_model)
            xgb_base_model = xgb.Booster()
            xgb_base_model.load_model(base_model)
            base_model_cfg = Path(base_model).with_suffix('.cfg')
            if os.path.exists(base_model_cfg):
                with open(base_model_cfg, 'r', encoding='utf-8') as f:
                    cfg = f.read()
                xgb_base_model.load_config(cfg)
            else:
                raise ValueError(f'Existing model config not found: {base_model_cfg}')
            feature_cols = xgb_base_model.feature_names   # align features to base model
        else:
            raise ValueError(f'Existing model not found for fine-tuning: {base_model}')
    xgb_model = train_model(features, feature_cols, label_col, n_trials=n_trials, base_model=xgb_base_model)

    # save model and params
    ensure_directory(model, parent=True)
    logger.info('Saving model to: %s', model)
    xgb_model.save_model(model)
    cfg = xgb_model.save_config()
    base_model_cfg = Path(model).with_suffix('.cfg')
    with open(base_model_cfg, 'w', encoding='utf-8') as f:
        f.write(cfg)

    ensure_directory(output_dir)

    for split in ['train', 'test']:
        if split == 'train':
            features_split = features[feature_cols].loc[features['split'].isin(['train', 'val'])]
        else:
            features_split = features[feature_cols].loc[features['split'] == split]
        if features_split.empty:
            continue

        feature_importance, _ = compute_shapley_values(xgb_model, features_split)
        feature_cols = feature_importance.feature.values
        feature_stats = features_split[feature_cols].describe().transpose().drop(columns='count')
        feature_stats = feature_stats.reset_index().rename(columns={'index': 'feature'})

        feature_importance = feature_importance.merge(feature_stats, how='left', on='feature')
        feature_importance.to_csv(f'{output_dir}/shap_{split}.csv')
        if split == 'train':
            # save training shap values and feature distribution metrics with model
            metrics_file = Path(model).with_suffix('.metrics')
            feature_importance.to_csv(metrics_file)

        print(f'Shapley feature importance and statistics ({split}):')
        print(tabulate(feature_importance, headers='keys', tablefmt='psql', floatfmt='.2f'))


def predict(
        platform: str,
        qual: str,
        output_info: dict,
        *,
        model: Optional[str] = None,
        qual_tool_filter: Optional[str] = 'stage') -> pd.DataFrame:
    """Predict GPU speedup given CPU logs."""

    node_level_supp, qual_tool_output, qual_metrics = _get_qual_data(qual)
    # create a DataFrame with default predictions for all app IDs.
    # this will be used for apps without predictions.
    default_preds_df = qual_tool_output.apply(create_row_with_default_speedup, axis=1)

    if len(qual_metrics) == 0:
        logger.warning('Qualification tool metrics are missing. Speedup predictions will be skipped.')
        return pd.DataFrame()

    # construct mapping of appIds to original appNames
    app_id_name_map = default_preds_df.set_index('appId')['appName'].to_dict()

    # if qualification metrics are provided, load metrics and apply filtering
    datasets = {}                       # create a dummy dataset
    dataset_name = Path(qual).name      # use qual directory name as dataset name
    datasets[dataset_name] = {
        'profiles': qual_metrics,
        'app_meta': {'default': {'runType': 'CPU', 'scaleFactor': 1}},
        'platform': platform,
    }

    logger.info('Loading dataset: %s', dataset_name)
    profile_df = load_profiles(
        datasets=datasets,
        node_level_supp=node_level_supp,
        qual_tool_filter=qual_tool_filter,
        qual_tool_output=qual_tool_output
    )
    if profile_df.empty:
        raise ValueError('Data preprocessing resulted in an empty dataset. Speedup predictions will default to 1.0.')

    # reset appName to original
    profile_df['appName'] = profile_df['appId'].map(app_id_name_map)

    # handle platform variants (for CPU eventlogs)
    variants = profile_df.loc[profile_df.runType == 'CPU']['sparkRuntime'].unique().tolist()
    if len(variants) > 1:
        # platform variants found in dataset
        if model and model != platform:
            # mixing platform and model args w/ variants is ambiguous
            raise ValueError(
                f'Platform variants found in data, but model ({model}) does not match platform ({platform}).'
            )
        # group input data by model variant
        prediction_groups = {}
        for variant in variants:
            xgb_model = _get_model(platform, None, variant)  # variants only supported for pre-trained platform models
            variant_profile_df = profile_df.loc[
                (profile_df['sparkRuntime'] == variant) | (profile_df['sparkRuntime'] == 'SPARK_RAPIDS')
            ]
            prediction_groups[xgb_model] = variant_profile_df
    else:
        # single platform
        xgb_model = _get_model(platform, model)
        prediction_groups = {
            xgb_model: profile_df
        }

    filter_str = (
        f'with {qual_tool_filter} filtering'
        if node_level_supp is not None and any(profile_df['fraction_supported'] != 1.0)
        else 'raw'
    )
    logger.info('Predicting dataset (%s): %s', filter_str, dataset_name)

    try:
        features_list = []
        predictions_list = []
        for xgb_model, prof_df in prediction_groups.items():
            features, feature_cols, label_col = extract_model_features(prof_df)
            features_index = features.index
            features_list.append(features)
            predictions = predict_model(xgb_model, features, feature_cols, label_col)
            predictions.index = features_index
            predictions_list.append(predictions)
        features = pd.concat(features_list)
        features.sort_index(inplace=True)
        predictions = pd.concat(predictions_list)
        predictions.sort_index(inplace=True)
    except XGBoostError as e:
        # ignore and continue
        logger.error(e)

    # write output files
    per_app_summary = None
    try:
        per_sql_summary = predictions

        # save features, feature importance, and shapley values per dataset name
        if output_info:
            # save features for troubleshooting
            output_file = output_info['features']['path']
            logger.info('Writing features to: %s', output_file)
            features.to_csv(output_file, index=False)

            feature_importance, shapley_values = compute_shapley_values(xgb_model, features)

            output_file = output_info['featureImportance']['path']
            logger.info('Writing shapley feature importances to: %s', output_file)
            feature_importance.to_csv(output_file)

            output_file = output_info['shapValues']['path']
            logger.info('Writing shapley values to: %s', output_file)
            shapley_values.to_csv(output_file, index=False)

        # compute per-app speedups
        summary = _compute_summary(per_sql_summary)
        # combine calculated summary with default predictions for missing apps
        per_app_summary = _add_entries_for_missing_apps(default_preds_df, summary)
        if INTERMEDIATE_DATA_ENABLED:
            print_summary(per_app_summary)
            # compute speedup for the entire dataset
            dataset_speedup = (
                per_app_summary['appDuration'].sum() / per_app_summary['appDuration_pred'].sum()
            )
            print(f'Dataset estimated speedup: {dataset_speedup:.2f}')
    except Exception as e:  # pylint: disable=broad-except
        # ignore and continue
        logger.error(e)
        traceback.print_exc()

    write_csv_reports(per_sql_summary, per_app_summary, output_info)
    if INTERMEDIATE_DATA_ENABLED:
        print_speedup_summary(per_app_summary)
    return per_app_summary


def _predict_cli(
    platform: str,
    output_dir: str,
    *,
    eventlogs: Optional[str] = None,
    qual_output: Optional[str] = None,
    model: Optional[str] = None,
    qual_tool_filter: Optional[str] = 'stage',
) -> None:
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
    output_dir:
        Path to save predictions as CSV files.
    eventlogs: str
        Path to a single event log, or a directory containing multiple event logs.
    qual_output: str
        Path to qualification tool output.
    model: str
        Either a model name corresponding to a platform/pre-trained model, or the path to an XGBoost
        model on disk.
    qual_tool_filter: str
        Set to either 'sqlID' or 'stage' (default) to apply model to supported sqlIDs or stages, based on qual tool
        output.  A sqlID or stage is fully supported if all execs are respectively fully supported.
    """
    # Note this function is for internal usage only. `spark_rapids predict` cmd is the public interface.
    assert eventlogs or qual_output, 'Please specify either --eventlogs or --qual_output.'

    # Construct output paths
    ensure_directory(output_dir)

    if eventlogs:
        # --eventlogs takes priority over --qual_output
        if not any(f.startswith('qual_') for f in os.listdir(output_dir)):
            # run qual tool if no existing qual output found
            run_qualification_tool(platform, eventlogs, output_dir)
        qual = output_dir
    else:
        qual = qual_output

    output_info = {
        'perSql': {'path': os.path.join(output_dir, 'per_sql.csv')},
        'perApp': {'path': os.path.join(output_dir, 'per_app.csv')},
        'shapValues': {'path': os.path.join(output_dir, 'shap_values.csv')},
        'features': {'path': os.path.join(output_dir, 'features.csv')},
        'featureImportance': {'path': os.path.join(output_dir, 'feature_importance.csv')},
    }

    predict(
        platform,
        output_info=output_info,
        qual=qual,
        model=model,
        qual_tool_filter=qual_tool_filter,
    )


def evaluate(
    platform: str,
    dataset: str,
    output_dir: str,
    *,
    model: Optional[str] = None,
    qual_tool_filter: Optional[str] = 'stage',
) -> None:
    """Evaluate model predictions against actual GPU speedup.

    For training datasets with GPU event logs, this returns the actual GPU speedup along with the
    predictions of:
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
    qual_tool_filter: str
        Set to either 'sqlID' or 'stage' (default) to apply model to supported sqlIDs or stages,
        based on qual tool output.  A sqlID or stage is fully supported if all execs are respectively
        fully supported.
    """
    with open(dataset, 'r', encoding='utf-8') as f:
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

    split_fn = None
    quals = os.listdir(qual_dir)
    for ds_name, ds_meta in datasets.items():
        if ds_name not in quals:
            # run qual tool if needed
            eventlogs = ds_meta['eventlogs']
            for eventlog in eventlogs:
                eventlog = os.path.expandvars(eventlog)
                run_qualification_tool(platform, eventlog, f'{qual_dir}/{ds_name}')
        if 'split_function' in ds_meta:
            # get split_function from plugin
            plugin_path = ds_meta['split_function']
            logger.info('Using split function for %s dataset from plugin: %s', ds_name, plugin_path)
            plugin = load_plugin(plugin_path)
            split_fn = plugin.split_function

    logger.info('Loading qualification tool CSV files.')
    node_level_supp, qual_tool_output, _ = _get_qual_data(qual_dir)

    logger.info('Loading profiler tool CSV files.')
    profile_df = load_profiles(datasets, profile_dir)  # w/ GPU rows
    filtered_profile_df = load_profiles(
        datasets, profile_dir, node_level_supp, qual_tool_filter, qual_tool_output
    )  # w/o GPU rows
    if profile_df.empty:
        raise ValueError(f'Warning: No profile data found for {dataset}')

    if not split_fn:
        # use default split_fn if not specified
        split_fn = split_all_test if 'test' in dataset_name else split_train_val

    # raw predictions on unfiltered data
    raw_sql, raw_app = _predict(
        xgb_model,
        dataset_name,
        profile_df,
        split_fn=split_fn,
        qual_tool_filter=qual_tool_filter,
    )

    # app level ground truth
    app_durations = (
        profile_df.loc[profile_df.runType == 'GPU'][
            ['appName', 'appDuration', 'description', 'scaleFactor']
        ]
        .groupby(['appName', 'appDuration', 'scaleFactor'])
        .first()
        .reset_index()
    )
    app_durations = app_durations.rename(columns={'appDuration': 'gpu_appDuration'})

    # join raw app data with app level gpu ground truth
    raw_app = raw_app.merge(
        app_durations[['appName', 'description', 'scaleFactor', 'gpu_appDuration']],
        on=['appName', 'description', 'scaleFactor'],
        how='left',
    )

    if not raw_app.loc[raw_app.gpu_appDuration.isna()].empty:
        logger.error(
            'missing gpu apps: %s', raw_app.loc[raw_app.gpu_appDuration.isna()].to_markdown()
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
        qual_tool_filter=qual_tool_filter,
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

    print(
        '\nComparison of qualx raw (QX) and qualx w/ stage filtering (QXS)'
    )
    print(tabulate(results_app, headers='keys', tablefmt='psql', floatfmt='.2f'))
    print()

    # compute mean abs percentage error (MAPE) for each tool (QX, QXS)

    score_dfs = []
    for granularity, split in [('sql', 'test'), ('sql', 'all'), ('app', 'all')]:
        res = results_app if granularity == 'app' else results_sql
        res = res[res.split == 'test'] if split == 'test' else res
        if res.empty:
            continue

        scores = compute_accuracy(
            res,
            'Actual speedup',
            {'QX': 'QX speedup', 'QXS': 'QXS speedup'},
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
    logger.info('Writing per-SQL predictions to: %s', sql_predictions_path)
    results_sql.to_csv(sql_predictions_path, index=False, na_rep='nan')

    app_predictions_path = os.path.join(output_dir, f'{dataset_name}_app.csv')
    logger.info('Writing per-application predictions to: %s', app_predictions_path)
    results_app.to_csv(app_predictions_path, index=False, na_rep='nan')


def evaluate_summary(
    evaluate: str,  # pylint: disable=W0621
    *,
    score: str = 'dMAPE',
    split: str = 'test',
) -> None:
    """
    Compute
    Parameters
    ----------
    evaluate: str
        Path to evaluation results directory.
    score: str
        Type of score to compare: 'MAPE', 'wMAPE', or 'dMAPE' (default).
    split: str
        Dataset split to compare: 'test' (default), 'train', or 'all'.
    """
    summary_df, _ = _read_platform_scores(evaluate, score, split)
    print(tabulate(summary_df, headers='keys', tablefmt='psql', floatfmt='.4f'))
    summary_path = f'{evaluate}/summary.csv'
    logger.info('Writing per-platform %s scores for \'%s\' split to: %s', score, split, summary_path)
    summary_df.to_csv(summary_path)


def compare(
    previous: str,
    current: str,
    *,
    score: str = 'dMAPE',
    granularity: str = 'sql',
    split: str = 'all',
) -> None:
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
        suffixes=('_prev', None),
    )
    for score_type in ['QX', 'QXS']:
        compare_df[f'{score_type}_delta'] = (
            compare_df[score_type] - compare_df[f'{score_type}_prev']
        )
    print(tabulate(compare_df, headers='keys', tablefmt='psql', floatfmt='.4f'))
    comparison_path = f'{current}/comparison_dataset.csv'
    logger.info('Writing dataset evaluation comparison to: %s', comparison_path)
    compare_df.to_csv(comparison_path)

    # compare app MAPE scores per platform
    # note: these aggregated scores can be impacted by the addition/removal of datasets
    # for removed datasets, just filter out from 'previous' aggregations
    # for added datasets, warn after printing table for more visibility
    curr_df, curr_datasets = _read_platform_scores(current, score, split)
    prev_df, prev_datasets = _read_platform_scores(
        previous, score, split, list(curr_datasets)
    )

    compare_df = prev_df.merge(
        curr_df,
        how='right',
        on=['model', 'platform'],
        suffixes=('_prev', None),
    )
    for score_type in ['QX', 'QXS']:
        compare_df[f'{score_type}_delta'] = (
            compare_df[score_type] - compare_df[f'{score_type}_prev']
        )
    print(tabulate(compare_df, headers='keys', tablefmt='psql', floatfmt='.4f'))
    comparison_path = f'{current}/comparison_platform.csv'
    logger.info('Writing platform evaluation comparison to: %s', comparison_path)
    compare_df.to_csv(comparison_path)

    # warn user of any new datasets
    added = curr_datasets - prev_datasets
    if added:
        logger.warning('New datasets added, comparisons may be skewed: added=%s', added)


def shap(platform: str, prediction_output: str, index: int, model: Optional[str] = None) -> None:
    """Print a SHAP waterfall of features and their SHAP value contributions to the overall prediction for
    a specific index (sqlID) in the shap_values.csv file produced by prediction.

    Output description:
    - features are listed in order of importance (absolute value of `shap_value`), similar to a SHAP waterfall plot.
    - `model_rank` shows the feature importance rank on the training set.
    - `model_shap_value` shows the feature shap_value on the training set.
    - `train_[mean|std|min|max]` show the mean, standard deviation, min and max values of the feature in the
    training set.
    - `train_[25%|50%|75%]` show the feature value at the respective percentile in the training set.
    - `feature_value` shows the value of the feature used in prediction (for the indexed row/sqlID).
    - `out_of_range` indicates if the `feature_value` used in prediction was outside of the range of values seen in
    the training set.
    - `Shap base value` is the model's average prediction across the entire training set.
    - `Shap values sum` is the sum of the `shap_value` column for this indexed instance.
    - `Shap prediction` is the sum of `Shap base value` and `Shap values sum`, representing the model's predicted value.
    - `exp(prediction)` is the exponential of `Shap prediction`, which represents the predicted speedup
    (since the XGBoost model currently predicts `log(speedup)`).
    - the predicted speedup (which should match `y_pred` in `per_sql.csv`) is applied to the "supported" durations
    and combined with the unsupported" durations to produce a final per-sql speedup (`speedup_pred` in `per_sql.csv`).

    Parameters
    ----------
    platform: str
        Platform used during prediction.
    prediction_output: str
        Path to prediction output directory containing predictions.csv and xgboost_predictions folder.
    index: int
        Index of row/instance in shap_values.csv file to isolate for shap waterfall.
    model: Optional[str]
        Path to XGBoost model used in prediction.
    """
    # get model shap values w/ feature distribution metrics
    model_json_path = _get_model_path(platform, model)
    model_shap_path = Path(model_json_path).with_suffix('.metrics')
    if os.path.exists(model_shap_path):
        logger.info('Reading model metrics from: %s', model_shap_path)
        model_shap_df = pd.read_csv(model_shap_path, index_col=0)
        model_shap_df = model_shap_df.reset_index().rename(
            columns={'index': 'model_rank', 'shap_value': 'model_shap_value'}
        )
        model_col_names = {
            c: 'train_' + c
            for c in model_shap_df.columns
            if c not in ['feature', 'model_rank', 'model_shap_value']
        }
        model_shap_df = model_shap_df.rename(columns=model_col_names)
    else:
        logger.info('No model metrics found for: %s', model_json_path)
        model_shap_df = pd.DataFrame()

    # get prediction shap values and isolate specific instance
    prediction_paths = find_paths(prediction_output, lambda f: f == 'shap_values.csv')
    if len(prediction_paths) == 1:
        prediction_dir = Path(prediction_paths[0]).parent
    elif len(prediction_paths) > 1:
        raise ValueError(f'Found multiple shap_values.csv files in: {prediction_output}')
    else:
        raise FileNotFoundError(f'File: shap_values.csv not found in: {prediction_output}')

    df = pd.read_csv(os.path.join(prediction_dir, 'shap_values.csv'))
    instance_shap = df.iloc[index]

    # extract the SHAP expected/base value of the model
    expected_value = instance_shap['expected_value']

    # convert instance to dataframe where rows are features
    instance_shap_df = (
        instance_shap.drop('expected_value')
        .to_frame(name='shap_value')
        .reset_index()
        .rename(columns={'index': 'feature'})
    )

    # get features used in prediction and isolate specific instance
    df = pd.read_csv(os.path.join(prediction_dir, 'features.csv'))
    instance_features = df.iloc[index]

    # convert instance to dataframe where rows are features
    instance_features_df = (
        instance_features.to_frame(name='feature_value')
        .reset_index()
        .rename(columns={'index': 'feature'})
    )

    # merge instance shap dataframe w/ model metrics (if available)
    if not model_shap_df.empty:
        instance_shap_df = instance_shap_df.merge(model_shap_df, how='left', on='feature')

    # merge instance shap dataframe w/ prediction features
    instance_shap_df = instance_shap_df.merge(instance_features_df, how='left', on='feature')

    # add out-of-range indicator (if model metrics available)
    if not model_shap_df.empty:
        instance_shap_df['out_of_range'] = (instance_shap_df['feature_value'] < instance_shap_df['train_min']) | (
            instance_shap_df['feature_value'] > instance_shap_df['train_max']
        )

    # sort by absolute value of shap_value
    instance_shap_df = instance_shap_df.sort_values('shap_value', ascending=False, key=abs)
    instance_shap_df.reset_index(drop=True, inplace=True)

    formats = {
        'feature': '',
        'shap_value': '0.4f',
        'model_rank': '',
        'model_shap_value': '0.4f',
        'train_mean': '0.1e',
        'train_std': '0.1e',
        'train_min': '0.1e',
        'train_25%': '0.1e',
        'train_50%': '0.1e',
        'train_75%': '0.1e',
        'train_max': '0.1e',
        'feature_value': '0.1e',
        'out_of_range': '',
    }
    format_list = [''] + [formats[col] for col in instance_shap_df.columns]  # index + cols

    # print instance shap values w/ model metrics
    print(tabulate(instance_shap_df, headers='keys', tablefmt='psql', floatfmt=format_list))

    # print shap prediction and final prediction
    shap_sum = instance_shap_df.shap_value.sum()
    prediction = expected_value + shap_sum
    print(f'Shap base value: {expected_value:.4f}')
    print(f'Shap values sum: {shap_sum:.4f}')
    print(f'Shap prediction: {prediction:.4f}')
    print(f'exp(prediction): {np.exp(prediction):.4f}')


def entrypoint() -> None:
    """
    These are commands are intended for internal usage only.
    """
    cmds = {
        'models': models,
        'preprocess': preprocess,
        'train': train,
        'predict': _predict_cli,
        'evaluate': evaluate,
        'evaluate_summary': evaluate_summary,
        'compare': compare,
        'shap': shap,
    }
    fire.Fire(cmds)


if __name__ == '__main__':
    entrypoint()
