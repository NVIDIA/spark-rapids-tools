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

"""High level pipeline for preprocessing, training, and evaluating QualX models in a more automated manner."""

from typing import List, Union
import glob
import itertools
import json
import os
import shutil
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd

from spark_rapids_tools.tools.qualx.config import get_config
from spark_rapids_tools.tools.qualx.qualx_config import QualxPipelineConfig
from spark_rapids_tools.tools.qualx.qualx_main import preprocess, train, evaluate
from spark_rapids_tools.tools.qualx.util import get_abs_path, get_logger, ensure_directory, find_paths
from spark_rapids_tools.storagelib.cspfs import CspFs, CspPath

logger = get_logger(__name__)


def _create_dataset_json(
        delta_df: pd.DataFrame,
        ds_eventlogs: List[str],
        datasets: str,
        platform: str,
        dataset_name: str,
        split_fn: Union[str, dict],
        filter_app_ids: bool = False) -> str:
    """Create a dataset JSON file from alignment CSV and eventlogs.

    Parameters
    ----------
    delta_df: pd.DataFrame
        DataFrame containing new rows of appId and sqlID alignments between CPU and GPU runs
    ds_eventlogs: List[str]
        Paths to directories in QUALX_DATA_DIR containing CPU and GPU eventlogs
    datasets: str
        Path to directory containing dataset JSON files
    platform: str
        Platform name (e.g. 'onprem', 'dataproc')
    dataset_name: str
        Base name for the dataset
    split_fn: Union[str, dict]
        Path to split function, or dictionary of path and args

    Returns
    -------
    str
        Path to the created dataset JSON file
    """
    # create app_meta mapping
    app_meta = {}
    for _, row in delta_df.iterrows():
        app_meta[row['appId_cpu']] = {'runType': 'CPU', 'scaleFactor': 1}
        app_meta[row['appId_gpu']] = {'runType': 'GPU', 'scaleFactor': 1}

    # get list of all CPU and GPU appIds
    cpu_app_ids = delta_df['appId_cpu'].unique()
    gpu_app_ids = delta_df['appId_gpu'].unique()
    app_ids = list(itertools.chain(cpu_app_ids, gpu_app_ids))

    # get list of all eventlogs for targeted CPU and GPU appIds
    # note: getting direct paths to files, since the parent directory may contain extra eventlogs
    eventlogs = []
    for path in ds_eventlogs:
        csp_path = CspPath(path)
        if filter_app_ids and csp_path.is_dir():
            matching_items = CspFs.glob_path(
                path=csp_path,
                pattern=app_ids,
                item_type=None,
                recursive=False
            )
            eventlogs.extend([str(item) for item in matching_items])
        else:
            eventlogs.append(str(csp_path))
    # remove duplicates
    eventlogs = sorted(list(set(eventlogs)))

    # create datasets/platform directory if it doesn't exist
    platform_dir = os.path.join(datasets, platform)
    ensure_directory(platform_dir)

    # create dataset path
    dataset_path = os.path.join(platform_dir, f'{dataset_name}.json')

    # create dataset JSON
    dataset_json = {
        dataset_name: {
            'eventlogs': eventlogs,
            'app_meta': dict(sorted(app_meta.items())),
            'platform': platform
        }
    }

    # set split function based on train flag
    dataset_json[dataset_name]['split_function'] = split_fn

    # write dataset JSON
    with open(dataset_path, 'w', encoding='utf-8') as f:
        json.dump(dataset_json, f, indent=4)

    logger.info('Created dataset JSON: %s', dataset_path)
    return dataset_path


def _unzip_eventlogs(
        delta_df: pd.DataFrame,
        cpu_eventlogs: List[str],
        gpu_eventlogs: List[str],
        dataset_basename: str,
        ds_name: str) -> List[str]:
    """Unzip the eventlogs to QUAL_DATA_DIR.

    Parameters
    ----------
    delta_df: pd.DataFrame
        DataFrame containing new rows of appId and sqlID alignments between CPU and GPU runs
    cpu_eventlogs: str
        Path to directory containing CPU eventlogs
    gpu_eventlogs: str
        Path to directory containing GPU eventlogs
    qualx_data_dir: str
        Path in QUAL_DATA_DIR to unzip the eventlogs to.
    ds_name: str
        Name of the dataset

    Returns
    -------
    List[str]
        Paths to the unzipped eventlogs
    """
    # get path to QUAL_DATA_DIR destination directory
    qualx_data_dir = os.getenv('QUALX_DATA_DIR')
    ds_eventlogs = []
    ds_eventlogs_base = os.path.join(qualx_data_dir, 'customers', dataset_basename, ds_name)
    if os.path.exists(os.path.expandvars(ds_eventlogs_base)):
        logger.warning('Eventlog directory already exists: %s, skipping unzip', ds_eventlogs_base)
        return ds_eventlogs

    # get appIds from delta_df
    gpu_app_ids = delta_df['appId_gpu'].unique()
    cpu_app_ids = delta_df['appId_cpu'].unique()

    # get eventlogs for new CPU and GPU appIds
    cpu_logs = []
    for path in cpu_eventlogs:
        cpu_logs.extend(find_paths(path, lambda f: any(app_id in f for app_id in cpu_app_ids)))
    gpu_logs = []
    for path in gpu_eventlogs:
        gpu_logs.extend(find_paths(path, lambda f: any(app_id in f for app_id in gpu_app_ids)))

    # unzip cpu eventlogs using zipfile
    cpu_eventlogs = os.path.join(ds_eventlogs_base, 'eventlogs', 'cpu')
    ds_eventlogs.append(cpu_eventlogs)
    ensure_directory(cpu_eventlogs, parent=True)
    logger.info('Unzipping eventlogs to %s', cpu_eventlogs)
    for cpu_log in cpu_logs:
        with zipfile.ZipFile(cpu_log, 'r') as zip_ref:
            logger.debug('Unzipping CPU eventlog: %s', cpu_log)
            zip_ref.extractall(cpu_eventlogs)

    # unzip gpu eventlogs using zipfile
    gpu_eventlogs = os.path.join(ds_eventlogs_base, 'eventlogs', 'gpu')
    ds_eventlogs.append(gpu_eventlogs)
    ensure_directory(gpu_eventlogs, parent=True)
    logger.info('Unzipping eventlogs to %s', gpu_eventlogs)
    for gpu_log in gpu_logs:
        with zipfile.ZipFile(gpu_log, 'r') as zip_ref:
            logger.debug('Unzipping GPU eventlog: %s', gpu_log)
            zip_ref.extractall(gpu_eventlogs)

    return ds_eventlogs


def train_and_evaluate(
    config: str,
) -> None:
    """Train and evaluate a model using aligned CPU and GPU eventlogs.

    This API supports incremental training by allowing multiple calls with new data.
    Each call will create a new dataset JSON file with an incrementing number.

    Config properties:
      - alignment_dir: Directory containing CSV files with CPU to GPU appId alignments
      - eventlogs: CPU and GPU eventlogs in zip format
      - platform: Platform name (e.g. 'onprem', 'dataproc')
      - dataset_name: Base name for the dataset

    Parameters
    ----------
    config: str
        Path to YAML config file containing training parameters.
    """
    # read config
    cfg = get_config(config, cls=QualxPipelineConfig, reload=True)
    # extract config values
    alignment_dir = get_abs_path(cfg.alignment_dir)
    cpu_eventlogs = cfg.eventlogs['cpu']
    gpu_eventlogs = cfg.eventlogs['gpu']
    zipped_eventlogs = cfg.eventlogs.get('zipped', True)
    filter_app_ids = cfg.eventlogs.get('filter_app_ids', False)
    datasets = get_abs_path(cfg.datasets)
    platform = cfg.platform
    dataset_basename = cfg.dataset_name
    model_type = cfg.model_type
    model_config = cfg.model_dump()[model_type]
    model_name = model_config['model_name']
    n_trials = model_config['n_trials']
    qual_tool_filter = model_config['qual_tool_filter']
    output_dir = cfg.output_dir

    model_path = f'{output_dir}/{model_type}/{model_name}'

    train_split_fn = cfg.split_functions['train']
    test_split_fn = cfg.split_functions['test']

    # check for inprogress alignment file
    inprogress_files = glob.glob(f'{alignment_dir}/{dataset_basename}_*.inprogress')
    if inprogress_files:
        inprogress_file = sorted(inprogress_files)[-1]
        logger.warning('In progress alignment file exists, re-running: %s', inprogress_file)
        suffix = Path(inprogress_file).stem.split('_')[-1]
        ds_name = f'{dataset_basename}_{suffix}'
        alignment_file = inprogress_file
    else:
        suffix = datetime.now().strftime('%Y%m%d%H%M%S')
        ds_name = f'{dataset_basename}_{suffix}'
        alignment_file = f'{alignment_dir}/{dataset_basename}.csv'
        inprogress_file = f'{alignment_dir}/{dataset_basename}_{suffix}.inprogress'

    # read alignment CSV
    alignment_df = pd.read_csv(alignment_file)
    required_cols = ['appId_cpu', 'appId_gpu']
    missing_cols = [col for col in required_cols if col not in alignment_df.columns]
    if missing_cols:
        raise ValueError(f'Alignment CSV missing required columns: {missing_cols}')

    # get previous alignment files, if exist
    prev_alignments = sorted(glob.glob(os.path.join(alignment_dir, f'{dataset_basename}_*.csv')))
    if len(prev_alignments) > 0:
        # load all previous alignment files, remove duplicates, and mark as processed
        prev_df = pd.concat([pd.read_csv(f) for f in prev_alignments])
        subset = [col for col in ['appId_cpu', 'sqlID_cpu'] if col in prev_df.columns]
        prev_df = prev_df.drop_duplicates(subset=subset, keep='last')
        prev_df['processed'] = 1

        # merge with current alignment file
        alignment_df = alignment_df.merge(
            prev_df,
            how='outer',
            on=list(alignment_df.columns),
        )
        delta_df = alignment_df.loc[alignment_df['processed'].isna()]
        delta_df = delta_df.drop(columns=['processed'])
    else:
        delta_df = alignment_df

    if delta_df.empty:
        logger.info('No new alignments found')
        return

    logger.info('New alignment rows: %d', len(delta_df))

    # for new alignment with new data, save deltas and mark as in progress
    if not inprogress_files:
        delta_df.to_csv(inprogress_file, index=False)

    # unzip and archive the eventlogs to QUAL_DATA_DIR
    if zipped_eventlogs:
        ds_eventlogs = _unzip_eventlogs(
            delta_df,
            cpu_eventlogs,
            gpu_eventlogs,
            dataset_basename,
            ds_name
        )
    else:
        ds_eventlogs = list(itertools.chain(cpu_eventlogs, gpu_eventlogs))

    # if trained model exists, evaluate new dataset against existing model
    if os.path.exists(model_path):
        dataset_json = _create_dataset_json(
            delta_df,
            ds_eventlogs,
            datasets,
            platform,
            ds_name,
            split_fn=test_split_fn,
            filter_app_ids=filter_app_ids
        )

        preprocess(datasets)

        # evaluate the previous model on the new dataset
        with open(dataset_json, 'r', encoding='utf-8') as f:
            logger.info('Evaluating %s model on %s', model_name, dataset_json)
            evaluate(
                platform=platform,
                dataset=dataset_json,
                output_dir=f'{output_dir}/evaluate',
                model=model_path,
                qual_tool_filter=qual_tool_filter
            )

        # archive the previous output directory with date suffix
        output_dir_archive = f'{output_dir}_{suffix}'
        logger.info('Archiving output directory: %s to: %s', output_dir, output_dir_archive)
        shutil.move(output_dir, output_dir_archive)

    logger.info('Adding new dataset to training')

    # create dataset JSON for training
    _create_dataset_json(
        delta_df,
        ds_eventlogs,
        datasets,
        platform,
        ds_name,
        split_fn=train_split_fn,
        filter_app_ids=filter_app_ids
    )

    # preprocess the data
    preprocess(datasets)

    # train the model
    train(
        dataset=datasets,
        model=model_path,
        n_trials=n_trials
    )

    # evaluate the model
    dataset_json_files = glob.glob(os.path.join(datasets, '**', '*.json'), recursive=True)
    for dataset_json in dataset_json_files:
        with open(dataset_json, 'r', encoding='utf-8') as f:
            logger.info('Evaluating %s model on %s', model_name, dataset_json)
            datasets = json.load(f)
            for ds_name in datasets.keys():
                evaluate(
                    platform=platform,
                    dataset=dataset_json,
                    output_dir=f'{output_dir}/evaluate',
                    model=model_path,
                    qual_tool_filter=qual_tool_filter
                )

    # TODO: compare model metrics against previous model
    # TODO: report model metrics
    # TODO: split new dataset into train/test/validation sets
    # TODO: re-train model and re-evaluate

    # mark completion by renaming the inprogress alignment file
    archive_file = inprogress_file.replace('.inprogress', '.csv')
    shutil.move(inprogress_file, archive_file)
