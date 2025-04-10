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

import glob
import json
import os
import shutil
import zipfile
from datetime import datetime
from pathlib import Path

import fire
import pandas as pd
import yaml

from spark_rapids_tools.tools.qualx.qualx_main import preprocess, train, evaluate
from spark_rapids_tools.tools.qualx.util import get_logger, ensure_directory, find_paths

logger = get_logger(__name__)


def _create_dataset_json(
        delta_df: pd.DataFrame,
        qualx_data_dir: str,
        datasets: str,
        platform: str,
        dataset_name: str,
        split_fn: str) -> str:
    """Create a dataset JSON file from alignment CSV and eventlogs.

    Parameters
    ----------
    delta_df: pd.DataFrame
        DataFrame containing new rows of appId and sqlID alignments between CPU and GPU runs
    qualx_data_dir: str
        Path to directory in QUALX_DATA_DIR containing CPU and GPU eventlogs
    datasets: str
        Path to directory containing dataset JSON files
    platform: str
        Platform name (e.g. 'onprem', 'dataproc')
    dataset_name: str
        Base name for the dataset
    split_fn: str
        Path to split function to use for the dataset

    Returns
    -------
    str
        Path to the created dataset JSON file
    """
    # Create app_meta mapping
    app_meta = {}
    for _, row in delta_df.iterrows():
        app_meta[row['appId_cpu']] = {'runType': 'CPU', 'scaleFactor': 1}
        app_meta[row['appId_gpu']] = {'runType': 'GPU', 'scaleFactor': 1}

    # Create datasets/platform directory if it doesn't exist
    platform_dir = os.path.join(datasets, platform)
    ensure_directory(platform_dir)

    dataset_path = os.path.join(platform_dir, f'{dataset_name}.json')
    # if os.path.exists(dataset_path):
    #     logger.warning('Dataset JSON already exists: %s', dataset_path)
    #     return dataset_path

    # Create dataset JSON
    dataset_json = {
        dataset_name: {
            'eventlogs': [qualx_data_dir],
            'app_meta': dict(sorted(app_meta.items())),
            'platform': platform
        }
    }

    # if alignment contains sqlID_cpu and sqlID_gpu, add alignment load_profiles_hook to dataset JSON
    if 'sqlID_cpu' in delta_df.columns and 'sqlID_gpu' in delta_df.columns:
        dataset_json[dataset_name]['load_profiles_hook'] = '${QUALX_DIR}/plugins/align_csv.py'
    else:
        # TODO: invoke hash alignment
        dataset_json[dataset_name]['load_profiles_hook'] = '${QUALX_DIR}/plugins/align_hash.py'

    # Set split function based on train flag
    dataset_json[dataset_name]['split_function'] = split_fn

    # Write dataset JSON
    with open(dataset_path, 'w', encoding='utf-8') as f:
        json.dump(dataset_json, f, indent=4)

    logger.info('Created dataset JSON: %s', dataset_path)
    return dataset_path


def _unzip_eventlogs(delta_df: pd.DataFrame, cpu_eventlogs: str, gpu_eventlogs: str, ds_path: str, ds_name: str) -> str:
    """Unzip the eventlogs to QUAL_DATA_DIR.

    Parameters
    ----------
    delta_df: pd.DataFrame
        DataFrame containing new rows of appId and sqlID alignments between CPU and GPU runs
    ds_path: str
        Path in QUAL_DATA_DIR to unzip the eventlogs to.
    ds_name: str
        Name of the dataset
    cpu_eventlogs: str
        Path to directory containing CPU eventlogs
    gpu_eventlogs: str
        Path to directory containing GPU eventlogs
    """
    # get appIds from delta_df
    gpu_app_ids = delta_df['appId_gpu'].unique()
    cpu_app_ids = delta_df['appId_cpu'].unique()

    # get eventlogs for new CPU and GPU appIds
    cpu_logs = find_paths(cpu_eventlogs, lambda f: any(app_id in f for app_id in cpu_app_ids))
    gpu_logs = find_paths(gpu_eventlogs, lambda f: any(app_id in f for app_id in gpu_app_ids))

    # Get path to QUAL_DATA_DIR destination directory
    eventlog_dir = os.path.join(os.path.expandvars(ds_path), ds_name)
    if os.path.exists(eventlog_dir):
        logger.warning('Eventlog directory already exists: %s', eventlog_dir)
        return eventlog_dir

    ensure_directory(eventlog_dir, parent=True)
    logger.info('Unzipping eventlogs to %s', eventlog_dir)
    # unzip cpu eventlogs using zipfile
    for cpu_log in cpu_logs:
        with zipfile.ZipFile(cpu_log, 'r') as zip_ref:
            logger.debug('Unzipping CPU eventlog: %s', cpu_log)
            zip_ref.extractall(f'{eventlog_dir}/cpu')

    # unzip gpu eventlogs using zipfile
    for gpu_log in gpu_logs:
        with zipfile.ZipFile(gpu_log, 'r') as zip_ref:
            logger.debug('Unzipping GPU eventlog: %s', gpu_log)
            zip_ref.extractall(f'{eventlog_dir}/gpu')

    return eventlog_dir


def train_and_evaluate(
    config: str,
) -> None:
    """Train and evaluate a model using aligned CPU and GPU eventlogs.

    This API supports incremental training by allowing multiple calls with new data.
    Each call will create a new dataset JSON file with an incrementing number.

    Config properties:
      - alignment: CSV file with columns: appId_cpu, appId_gpu, (sqlID_cpu, sqlID_gpu)
      - eventlogs: CPU and GPU eventlogs in zip format
      - platform: Platform name (e.g. 'onprem', 'dataproc')
      - dataset_name: Base name for the dataset

    Parameters
    ----------
    config: str
        Path to YAML config file containing training parameters.
    """
    # Read YAML config
    cfg_dir = Path(config).parent
    with open(config, 'r', encoding='utf-8') as f:
        cfg = yaml.safe_load(f)

    def _get_abs_path(cwd: str, path: str) -> str:
        if not path.startswith('/'):
            return os.path.join(cwd, path)
        return path

    # Extract config values
    alignment_file = _get_abs_path(cfg_dir, cfg['alignment_file'])
    cpu_eventlogs = _get_abs_path(cfg_dir, cfg['eventlogs']['cpu']) if 'eventlogs' in cfg else None
    gpu_eventlogs = _get_abs_path(cfg_dir, cfg['eventlogs']['gpu']) if 'eventlogs' in cfg else None
    output_dir = _get_abs_path(cfg_dir, cfg['output_dir'])
    datasets = _get_abs_path(cfg_dir, cfg['datasets'])
    platform = cfg['platform']
    dataset_basename = cfg['dataset_name']
    qual_data_path = cfg['qualx_data_path']
    model_type = cfg['model_type']
    model_name = cfg[model_type]['model_name']
    n_trials = cfg['qualx']['n_trials']
    qual_tool_filter = cfg['qualx']['qual_tool_filter']
    train_split_fn = _get_abs_path(cfg_dir, cfg['split_functions']['train'])
    test_split_fn = _get_abs_path(cfg_dir, cfg['split_functions']['test'])

    alignment_dir = Path(alignment_file).parent
    alignment_basename = Path(alignment_file).stem
    model_path = f'{output_dir}/{model_type}/{model_name}'

    # Check for inprogress alignment file
    inprogress_files = glob.glob(f'{alignment_dir}/{alignment_basename}_*.inprogress')
    if inprogress_files:
        inprogress_file = sorted(inprogress_files)[-1]
        logger.warning('In progress alignment file exists, re-running: %s', inprogress_file)
        suffix = Path(inprogress_file).stem.split('_')[-1]
        ds_name = f'{dataset_basename}_{suffix}'
        alignment_file = inprogress_file
    else:
        suffix = datetime.now().strftime('%Y%m%d%H%M%S')
        ds_name = f'{dataset_basename}_{suffix}'
        inprogress_file = f'{alignment_dir}/{alignment_basename}_{suffix}.inprogress'

    # Read alignment CSV
    alignment_df = pd.read_csv(alignment_file)
    required_cols = ['appId_cpu', 'appId_gpu']
    missing_cols = [col for col in required_cols if col not in alignment_df.columns]
    if missing_cols:
        raise ValueError(f'Alignment CSV missing required columns: {missing_cols}')

    # Get previous alignment file, if exists
    prev_alignments = glob.glob(os.path.join(alignment_dir, f'{alignment_basename}_*.csv'))
    if len(prev_alignments) > 0:
        # Get new rows between last alignment file and current alignment file
        last_alignment = sorted(prev_alignments)[-1]
        last_df = pd.read_csv(last_alignment)
        last_df['processed'] = 1
        delta_df = alignment_df.merge(
            last_df[['appId_cpu', 'appId_gpu', 'processed']],
            how='outer',
            on=['appId_cpu', 'appId_gpu'],
        )
        delta_df = delta_df.loc[delta_df['processed'].isna()]
        delta_df = delta_df.drop(columns=['processed'])
    else:
        delta_df = alignment_df

    if delta_df.empty:
        logger.info('No new alignments found')
        return

    # For new alignment with new data, mark as in progress
    if not inprogress_files:
        shutil.copy(alignment_file, inprogress_file)

    # Unzip and archive the eventlogs to QUAL_DATA_DIR
    if cpu_eventlogs and gpu_eventlogs:
        qual_data_dir = _unzip_eventlogs(delta_df, cpu_eventlogs, gpu_eventlogs, qual_data_path, ds_name)
    else:
        qual_data_dir = qual_data_path

    # If trained model exists, evaluate new dataset against existing model
    if os.path.exists(model_path):
        dataset_json = _create_dataset_json(
            delta_df,
            qual_data_dir,
            datasets,
            platform,
            ds_name,
            split_fn=test_split_fn
        )

        preprocess(datasets)

        # Evaluate the previous model on the new dataset
        with open(dataset_json, 'r', encoding='utf-8') as f:
            logger.info('Evaluating %s model on %s', model_name, dataset_json)
            dataset = json.load(f)
            for ds_name in dataset.keys():
                evaluate(
                    platform=platform,
                    dataset=dataset_json,
                    output_dir=f'{output_dir}/evaluate',
                    model=model_path,
                    qual_tool_filter=qual_tool_filter
                )

        # archive the previous output directory with date suffix
        output_dir_archive = f'{output_dir}_{suffix}'
        shutil.move(output_dir, output_dir_archive)

    # Create dataset JSON for training
    _create_dataset_json(
        delta_df,
        qual_data_dir,
        datasets,
        platform,
        ds_name,
        split_fn=train_split_fn
    )

    # Preprocess the data
    preprocess(datasets)

    # Train the model
    train(
        dataset=datasets,
        model=model_path,
        n_trials=n_trials
    )

    # Evaluate the model
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

    # TODO: Compare model metrics against previous model
    # TODO: Report model metrics
    # TODO: Split new dataset into train/test/validation sets
    # TODO: Re-train model and re-evaluate

    # Mark completion by renaming the inprogress alignment file
    archive_file = inprogress_file.replace('.inprogress', '.csv')
    shutil.move(inprogress_file, archive_file)


if __name__ == '__main__':
    fire.Fire(train_and_evaluate)
