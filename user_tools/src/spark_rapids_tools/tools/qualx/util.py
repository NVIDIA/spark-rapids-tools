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

from concurrent.futures import ThreadPoolExecutor, as_completed
import glob
import logging
import os
import re
from typing import Tuple
import secrets
import string
import subprocess
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from tabulate import tabulate

INTERMEDIATE_DATA_ENABLED = False
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')


def get_logger(name: str):
    return logging.getLogger(name)


logger = get_logger(__name__)


class RegexPattern:
    appId = re.compile(r'^app.*[_-][0-9]+[_-][0-9]+$')
    profile = re.compile(r'^prof_[0-9]+_[0-9a-zA-Z]+$')
    qualtool = re.compile(r'^qual_[0-9]+_[0-9a-zA-Z]+$')
    rapidsProfile = re.compile(r'rapids_4_spark_profile')
    rapidsQualtool = re.compile(r'rapids_4_spark_qualification_output')


def ensure_directory(path, parent=False):
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


def find_paths(dir, filter_fn=None, return_directories=False):
    """Find all files or subdirectories in a directory that match a filter function.

    Parameters
    ----------
    dir: str
        Path to directory to search.
    filter_fn: Callable
        Filter function that selects files/directories.
    return_directories: bool
        If true, returns matching directories, otherwise returns matching files
    """
    paths = []
    if dir and os.path.isdir(dir):
        for root, dirs, files in os.walk(dir):
            if return_directories:
                filtered_dirs = filter(filter_fn, dirs)
                paths.extend([os.path.join(root, dir) for dir in filtered_dirs])
            else:
                filtered_files = filter(filter_fn, files)
                paths.extend([os.path.join(root, file) for file in filtered_files])
    return paths


def find_eventlogs(path) -> list[str]:
    """Find all eventlogs given a root directory."""
    if '*' in path:
        # handle paths w/ glob patterns
        eventlogs = [os.path.join(path, f) for f in glob.glob(path, recursive=True)]
    elif Path(path).is_dir:
        # find all 'eventlog' files in directory
        eventlogs = find_paths(path, lambda f: f == 'eventlog')
        if eventlogs:
            # and return their parent directories
            eventlogs = [Path(p).parent for p in eventlogs]
        else:
            # otherwise, find all 'app-XXXX-YYYY' files
            eventlogs = find_paths(path, RegexPattern.appId.match(path))
            if not eventlogs:
                raise ValueError(f'No event logs found in: {path}')
    else:
        # if file, just return as list
        eventlogs = [path]

    return eventlogs


def get_cache_dir():
    return os.environ.get('QUALX_CACHE_DIR', 'qualx_cache')


def get_dataset_platforms(dataset: str) -> Tuple[list[str], str]:
    """Return list of platforms and dataset parent directory from a string path.

    Parameters
    ----------
    dataset: str
        Path to datasets directory, datasets/platform directory, or datasets/platform/dataset.json file.
    """
    splits = Path(dataset).parts
    platform = splits[-1]
    if platform.endswith('.json'):
        # dataset JSON, assume parent dir is platform
        platforms = [splits[-2]]
        dataset_base = os.path.join(*splits[:-2])
    elif platform == 'datasets':
        # all datasets, assume directory contains platforms
        platforms = os.listdir(dataset)
        dataset_base = dataset
    else:
        # default, last component is platform
        platforms = [platform]
        dataset_base = os.path.join(*splits[:-1])
    return platforms, dataset_base


def compute_accuracy(
    results: pd.DataFrame, y: str, y_preds: dict[str, str], weight: str = None
):
    """Compute accuracy scores on a pandas DataFrame.

    Parameters
    ----------
    results: pd.DataFrame
        Pandas dataframe containing the label column and one or more prediction columns.
    y: str
        Label column name.
    y_preds: dict[str, str]
        Dictionary of display name of metric -> prediction column name.
    weight: str
        Name of column to use as weight, e.g. 'Duration' or 'appDuration'.
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


def random_string(length: int) -> str:
    """Return a random hexadecimal string of a specified length."""
    return ''.join(secrets.choice(string.hexdigits) for _ in range(length))


def run_profiler_tool(platform: str, eventlog: str, output_dir: str):
    ts = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
    output = f'{output_dir}/prof_{ts}'
    logger.info(f'Running profiling on: {eventlog}')
    logger.info(f'Saving output to: {output_dir}')

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
            f'--platform {platform} --csv -o {output} {log}'
        )
        cmds.append(cmd)
    run_commands(cmds)


def run_qualification_tool(platform: str, eventlog: str, output_dir: str):
    ts = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
    logger.info(f'Running qualification on: {eventlog}')
    logger.info(f'Saving output to: {output_dir}')

    cmds = []
    eventlogs = find_eventlogs(eventlog)
    for log in eventlogs:
        # skip gpu logs, assuming /gpu appearing in path can be used to distinguis
        if '/gpu' in str(log).lower():
            continue
        suffix = random_string(6)
        output = f'{output_dir}/qual_{ts}_{suffix}'
        cmd = (
            # f'spark_rapids_user_tools {platform} qualification --csv --per-sql --eventlogs {log} --local_folder {output}'
            'java -Xmx32g -cp $SPARK_RAPIDS_TOOLS_JAR:$SPARK_HOME/jars/*:$SPARK_HOME/assembly/target/scala-2.12/jars/* '
            'com.nvidia.spark.rapids.tool.qualification.QualificationMain '
            f'--platform {platform} --per-sql -o {output} {log}'
        )
        cmds.append(cmd)
    run_commands(cmds)


def run_commands(commands: list[str], workers: int = 8):
    """Run a list of commands using a thread pool."""
    if not commands:
        return

    def run_command(command: str):
        logger.debug(f'Command started: {command}')
        result = subprocess.run(
            command, shell=True, env=os.environ, capture_output=True, text=True
        )
        return result

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(run_command, command): command for command in commands
        }

        # iterate over completed futures as they become available
        for future in as_completed(futures):
            command = futures[future]
            try:
                result = future.result()
                logger.debug(f'Command completed: {command}')
                logger.info(result.stdout)
                logger.info(result.stderr)
            except Exception as e:
                logger.error(f'Command failed: {command}')
                logger.error(e)


def print_summary(summary):
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
        # qual
        'Estimated GPU Speedup': 'Estimated Qualtool\nGPU Speedup',
    }
    col_map = {k: v for k, v in display_cols.items() if k in summary.columns}
    formatted = summary[col_map.keys()].rename(col_map, axis=1)
    print(tabulate(formatted, headers='keys', tablefmt='psql', floatfmt='.2f'))


def print_speedup_summary(dataset_summary: pd.DataFrame):
    overall_speedup = (
            dataset_summary["appDuration"].sum()
            / dataset_summary["appDuration_pred"].sum()
    )
    total_applications = dataset_summary.shape[0]
    summary = {
        "Total applications": total_applications,
        "Overall estimated speedup": overall_speedup,
    }
    summary_df = pd.DataFrame(summary, index=[0]).transpose()
    print("\nReport Summary:")
    print(tabulate(summary_df, colalign=("left", "right")))
    print()
