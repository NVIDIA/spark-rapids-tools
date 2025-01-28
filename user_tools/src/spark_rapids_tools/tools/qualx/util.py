# Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

""" Utility functions for QualX """

from dataclasses import dataclass
from typing import Dict, List, Tuple, Callable
import glob
import importlib
import logging
import os
import re
import secrets
import string
import types
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from tabulate import tabulate
import numpy as np
import pandas as pd

INTERMEDIATE_DATA_ENABLED = False
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


logger = get_logger(__name__)


@dataclass
class RegexPattern:
    app_id = re.compile(r'app.*[_-][0-9]+[_-][0-9]+')
    profile = re.compile(r'prof_[0-9]+_[0-9a-zA-Z]+')
    qual_tool = re.compile(r'qual_[0-9]+_[0-9a-zA-Z]+')
    rapids_profile = re.compile(r'rapids_4_spark_profile')
    rapids_qual = re.compile(r'rapids_4_spark_qualification_output')
    qual_tool_metrics = re.compile(r'raw_metrics')


def ensure_directory(path: str, parent: bool = False) -> None:
    """Ensure that a directory exists for a given path.

    Parameters
    ----------
    path: str
        Path to directory.
    parent: bool
        If true, ensure that the parent directory exists.
    """
    ensure_path = Path(path)
    if parent:
        ensure_path = ensure_path.parent
    os.makedirs(ensure_path, exist_ok=True)


def find_paths(directory: str, filter_fn: Callable = None, return_directories: bool = False) -> List[str]:
    """Find all files or subdirectories in a directory that match a filter function.

    Parameters
    ----------
    directory: str
        Path to directory to search.
    filter_fn: Callable
        Filter function that selects files/directories.
    return_directories: bool
        If true, returns matching directories, otherwise returns matching files
    """
    paths = []
    if directory and os.path.isdir(directory):
        for root, directories, files in os.walk(directory):
            if return_directories:
                filtered_dirs = filter(filter_fn, directories)
                paths.extend([os.path.join(root, filtered_dir) for filtered_dir in filtered_dirs])
            else:
                filtered_files = filter(filter_fn, files)
                paths.extend([os.path.join(root, file) for file in filtered_files])
    return paths


def find_eventlogs(path: str) -> List[str]:
    """Find all eventlogs given a root directory."""
    if '*' in path:
        # handle paths w/ glob patterns
        eventlogs = [os.path.join(path, f) for f in glob.glob(path, recursive=True)]
    elif Path(path).is_dir():
        # find all 'eventlog' files in directory
        eventlogs = find_paths(path, lambda f: f == 'eventlog')
        if eventlogs:
            # and return their parent directories
            eventlogs = [Path(p).parent for p in eventlogs]
        else:
            # otherwise, find all 'app-XXXX-YYYY' files
            eventlogs = find_paths(path, RegexPattern.app_id.match(path))
            if not eventlogs:
                raise ValueError(f'No event logs found in: {path}')
    else:
        # if file, just return as list
        eventlogs = [path]

    return eventlogs


def get_cache_dir() -> str:
    return os.environ.get('QUALX_CACHE_DIR', 'qualx_cache')


def get_dataset_platforms(dataset: str) -> Tuple[List[str], str]:
    """Return list of platforms and dataset parent directory from a string path.

    Note: the '_' character is reserved to delimit variants of a platform, e.g. different execution engines
    like Photon or Velox.  Example: `databricks-aws_photon`.

    Platform variants may be used when separate qualx models are trained for the base platform and the variant.
    The platform variant will be used to identify the qualx model, while the base platform will be used when
    invoking the Profiler/Qualification tools.

    Parameters
    ----------
    dataset: str
        Path to datasets directory, datasets/platform directory, or datasets/platform/dataset.json file.

    Returns
    -------
    platforms: List[str]
        List of platforms associated with the dataset path.
    dataset_base: str
        Parent directory of datasets directory.
    """
    supported_platforms = [
        'databricks-aws',
        'databricks-aws_photon',
        'databricks-azure',
        'databricks-azure_photon',
        'dataproc',
        'emr',
        'onprem'
    ]

    splits = Path(dataset).parts
    basename = splits[-1]
    if basename.endswith('.json'):
        # single dataset JSON
        platforms = [splits[-2]]
        dataset_base = os.path.join(*splits[:-2])
    elif basename in supported_platforms:
        # single platform directory
        platforms = [basename]
        dataset_base = os.path.join(*splits[:-1])
    else:
        # otherwise, assume "datasets" directory
        platforms = os.listdir(dataset)
        dataset_base = dataset

    # validate platforms
    for platform in platforms:
        if platform not in supported_platforms:
            raise ValueError(f'Unsupported platform: {platform}')

    return platforms, dataset_base


def compute_accuracy(
    results: pd.DataFrame, y: str, y_preds: Dict[str, str], weight: str = None
) -> Dict[str, Dict[str, float]]:
    """Compute accuracy scores on a pandas DataFrame.

    Parameters
    ----------
    results: pd.DataFrame
        Pandas dataframe containing the label column and one or more prediction columns.
    y: str
        Label column name.
    y_preds: Dict[str, str]
        Dictionary of display name of metric -> prediction column name.
    weight: str
        Name of column to use as weight, e.g. 'Duration' or 'appDuration'.

    Returns
    -------
    scores: Dict[str, Dict[str, float]]
        Dictionary of different scoring metrics per prediction column,
        e.g. {'QXS': {'MAPE"; 1.31, 'dMAPE': 1.25}}
    """
    scores = {}
    for name, y_pred in y_preds.items():
        scores[name] = {
            'MAPE': np.average(np.abs(results[y] - results[y_pred]) / results[y]),
            'wMAPE': np.sum(np.abs(results[y] - results[y_pred])) / np.sum(results[y]),
        }
        if weight:
            # MAPE w/ custom weighting by duration
            scores[name]['dMAPE'] = np.sum(
                results[weight] * (np.abs(results[y] - results[y_pred]) / results[y])
            ) / np.sum(results[weight])

            # MAPE w/ custom weighting by log(1 + duration)
            scores[name]['ldMAPE'] = np.sum(
                np.log(1 + results[weight])
                * (np.abs(results[y] - results[y_pred]) / results[y])
            ) / np.sum(np.log(1 + results[weight]))

            # wMAPE w/ custom weighting by duration
            scores[name]['dwMAPE'] = np.sum(
                results[weight] * np.abs(results[y] - results[y_pred])
            ) / (np.sum(results[weight] * results[y]))

            # wMAPE w/ custom weighting by log(1 + duration)
            scores[name]['ldwMAPE'] = np.sum(
                np.log(1 + results[weight]) * np.abs(results[y] - results[y_pred])
            ) / (np.sum(np.log(1 + results[weight]) * results[y]))
    return scores


def compute_precision_recall(
    results: pd.DataFrame, y: str, y_preds: Dict[str, str], threshold: float
) -> Tuple[Dict[str, float], Dict[str, float]]:
    """Compute precision and recall from a dataframe using a threshold for identifying true positives.

    Parameters
    ----------
    results: pd.DataFrame
        Pandas dataframe containing the label column and one or more prediction columns.
    y: str
        Label column name.
    y_preds: Dict[str, str]
        Dictionary of display name of prediction -> prediction column name.
    threshold: float
        Threshold separating positives (inclusive of threshold) from negatives.

    Returns
    -------
    precision: Dict[str, float]
        Dictionary of precision metric per prediction column,
        e.g. {'QX': 0.90, 'QXS': 0.92}
    recall: Dict[str, float]
        Dictionary of recall metric per prediction column,
        e.g. {'QX': 0.78, 'QXS': 0.82}
    """
    precision = {}
    recall = {}
    for name, y_pred in y_preds.items():
        tp = sum((results[y_pred] >= threshold) & (results[y] >= threshold))
        fp = sum((results[y_pred] >= threshold) & (results[y] < threshold))
        fn = sum((results[y_pred] < threshold) & (results[y] >= threshold))

        precision[name] = tp / (tp + fp) if (tp + fp) > 0 else np.nan
        recall[name] = tp / (tp + fn) if (tp + fn) > 0 else np.nan

    return precision, recall


def load_plugin(plugin_path: str) -> types.ModuleType:
    """Dynamically load plugin modules with helper functions for dataset-specific code.

    Supported APIs:

    def load_profiles_hook(df: pd.DataFrame) -> pd.DataFrame:
        # add dataset-specific modifications
        return df
    """
    plugin_path = os.path.expandvars(plugin_path)
    plugin_name = Path(plugin_path).name.split('.')[0]
    if not os.path.exists(plugin_path):
        raise FileNotFoundError(f'Plugin not found: {plugin_path}')

    spec = importlib.util.spec_from_file_location(plugin_name, plugin_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    logger.info('Successfully loaded plugin: %s', plugin_path)
    return module


def random_string(length: int) -> str:
    """Return a random hexadecimal string of a specified length."""
    return ''.join(secrets.choice(string.hexdigits) for _ in range(length))


def run_profiler_tool(platform: str, eventlog: str, output_dir: str) -> None:
    ts = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
    logger.info('Running profiling on: %s', eventlog)
    logger.info('Saving output to: %s', output_dir)

    platform_base = platform.split('_')[0]  # remove any platform variants when invoking profiler
    cmds = []
    eventlogs = find_eventlogs(eventlog)
    for log in eventlogs:
        logfile = os.path.basename(log)
        match = re.search('sf[0-9]+[k]*-', logfile)
        if match:
            output = f'{output_dir}/{logfile}'
        else:
            suffix = random_string(6)
            output = f'{output_dir}/prof_{ts}_{suffix}'
        cmd = (
            # f'spark_rapids_user_tools {platform} profiling --csv --eventlogs {log} --local_folder {output}'
            'java -Xmx64g -cp $SPARK_RAPIDS_TOOLS_JAR:$SPARK_HOME/jars/*:$SPARK_HOME/assembly/target/scala-2.12/jars/* '
            'com.nvidia.spark.rapids.tool.profiling.ProfileMain '
            f'--platform {platform_base} --csv --output-sql-ids-aligned -o {output} {log}'
        )
        cmds.append(cmd)
    run_commands(cmds)


def run_qualification_tool(platform: str, eventlog: str, output_dir: str) -> None:
    ts = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
    logger.info('Running qualification on: %s', eventlog)
    logger.info('Saving output to: %s', output_dir)

    platform_base = platform.split('_')[0]  # remove any platform variants when invoking qualification
    cmds = []
    eventlogs = find_eventlogs(eventlog)
    for log in eventlogs:
        # skip gpu logs, assuming /gpu appearing in path can be used to distinguis
        if '/gpu' in str(log).lower():
            continue
        suffix = random_string(6)
        output = f'{output_dir}/qual_{ts}_{suffix}'
        cmd = (
            # f'spark_rapids_user_tools {platform} qualification --csv
            # --per-sql --eventlogs {log} --local_folder {output}'
            'java -Xmx32g -cp $SPARK_RAPIDS_TOOLS_JAR:$SPARK_HOME/jars/*:$SPARK_HOME/assembly/target/scala-2.12/jars/* '
            'com.nvidia.spark.rapids.tool.qualification.QualificationMain '
            f'--platform {platform_base} --per-sql -o {output} {log}'
        )
        cmds.append(cmd)
    run_commands(cmds)


def run_commands(commands: List[str], workers: int = 8) -> None:
    """Run a list of commands using a thread pool."""
    if not commands:
        return

    def run_command(command: str) -> subprocess.CompletedProcess:
        logger.debug('Command started: %s', command)
        return subprocess.run(
            command, shell=True, env=os.environ, capture_output=True, text=True, check=False
        )

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(run_command, command): command for command in commands
        }

        # iterate over completed futures as they become available
        for future in as_completed(futures):
            command = futures[future]
            try:
                result = future.result()
                logger.debug('Command completed: %s', command)
                logger.info(result.stdout)
                logger.info(result.stderr)
            except Exception as e:  # pylint: disable=broad-except
                logger.error('Command failed: %s', command)
                logger.error(e)


def print_summary(summary: pd.DataFrame) -> None:
    # print summary as formatted table
    display_cols = {
        # qualx
        'appName': 'App Name',
        'appId': 'App ID',
        'appDuration': 'App Duration',
        'Duration': 'SQL Duration',
        'Duration_supported': 'Estimated Supported\nSQL Duration',
        'Duration_pred': 'Estimated GPU\nSQL Duration',
        'appDuration_pred': 'Estimated GPU\nApp Duration',
        'fraction_supported': 'Estimated Supported\nSQL Duration Fraction',
        'speedup': 'Estimated GPU\nSpeedup',
        # actual
        'gpuDuration': 'Actual GPU\nSQL Duration',
        'appDuration_actual': 'Actual GPU\nApp Duration',
        'speedup_actual': 'Actual GPU\nSpeedup',
    }
    col_map = {k: v for k, v in display_cols.items() if k in summary.columns}
    formatted = summary[col_map.keys()].rename(col_map, axis=1)
    print(tabulate(formatted, headers='keys', tablefmt='psql', floatfmt='.2f'))


def print_speedup_summary(dataset_summary: pd.DataFrame) -> None:
    overall_speedup = (
            dataset_summary['appDuration'].sum()
            / dataset_summary['appDuration_pred'].sum()
    )
    total_applications = dataset_summary.shape[0]
    summary = {
        'Total applications': total_applications,
        'Overall estimated speedup': overall_speedup,
    }
    summary_df = pd.DataFrame(summary, index=[0]).transpose()
    print('\nReport Summary:')
    print(tabulate(summary_df, colalign=('left', 'right')))
    print()


def create_row_with_default_speedup(app: pd.Series) -> pd.Series:
    """
    Create a default row for an app with no speedup prediction.
    """
    return pd.Series({
        'appName': app['App Name'],
        'appId': app['App ID'],
        'appDuration': app['App Duration'],
        'Duration': 0,
        'Duration_pred': 0,
        'Duration_supported': 0,
        'fraction_supported': 0.0,
        'appDuration_pred': app['App Duration'],
        'speedup': 1.0,
    })


def write_csv_reports(per_sql: pd.DataFrame, per_app: pd.DataFrame, output_info: dict) -> None:
    """
    Write per-SQL and per-application predictions to CSV files
    """
    try:
        if per_sql is not None:
            sql_predictions_path = output_info['perSql']['path']
            logger.info('Writing per-SQL predictions to: %s', sql_predictions_path)
            per_sql.to_csv(sql_predictions_path)
    except Exception as e:  # pylint: disable=broad-except
        logger.error('Error writing per-SQL predictions. Reason: %s', e)

    try:
        if per_app is not None:
            app_predictions_path = output_info['perApp']['path']
            logger.info('Writing per-application predictions to: %s', app_predictions_path)
            per_app.to_csv(app_predictions_path)
    except Exception as e:  # pylint: disable=broad-except
        logger.error('Error writing per-app predictions. Reason: %s', e)


def log_fallback(logger_obj: logging.Logger, app_ids: List[str], fallback_reason: str) -> None:
    """
    Log a warning message for a fallback during preprocessing.
    This function expects logger object to log the source module of the warning.
    """
    app_id_str = ', '.join(app_ids)
    logger_obj.warning(f'Predicted speedup will be 1.0 for {app_id_str}. Reason: {fallback_reason}.')
