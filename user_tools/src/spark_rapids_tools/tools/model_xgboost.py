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

"""XGBoost model for predicting GPU speedup for Spark SQL queries."""

from dataclasses import dataclass
import glob
import logging
import os
import re
import traceback
from pathlib import Path
from typing import Optional, Mapping, List, Dict, Callable, Tuple

import numpy as np
import pandas as pd
import xgboost as xgb
import shap
from tabulate import tabulate
from xgboost.core import XGBoostError

from spark_rapids_pytools.common.utilities import Utils

logger = logging.getLogger(__name__)
FILTER_SPILLS = False  # remove queries with any disk/mem spills
LOG_LABEL = True  # use log(y) as target
INTERMEDIATE_DATA_ENABLED = False


class ScanTblError(Exception):
    pass


# expected features for dataframe produced by preprocessing
expected_raw_features = set(
    [
        'appId',
        'appDuration',
        'appName',
        'cache_hit_ratio',
        'data_size',
        'decode_time',
        'description',
        'diskBytesSpilled_sum',
        'diskBytesSpilledBool',
        'diskBytesSpilledRatio',
        'duration_max',
        'duration_mean',
        'duration_min',
        'duration_sum',
        'Duration',
        'executorCores',
        'executorCPUTime_sum',
        'executorDeserializeCPUTime_sum',
        'executorDeserializeTime_sum',
        'executorMemory',
        'executorOffHeap',
        'executorRunTime_sum',
        'fraction_supported',
        'hasSqlID',
        'input_bytesRead_sum',
        'input_bytesReadRatio',
        'input_recordsRead_sum',
        'jobStartTime_min',
        'jvmGCTime_sum',
        'maxMem',
        'maxOffHeapMem',
        'maxOnHeapMem',
        'memoryBytesSpilled_sum',
        'memoryBytesSpilledBool',
        'memoryBytesSpilledRatio',
        'numExecutors',
        'numGpusPerExecutor',
        'numTasks_sum',
        'output_bytesWritten_sum',
        'output_bytesWrittenRatio',
        'output_recordsWritten_sum',
        'peakExecutionMemory_max',
        'pluginEnabled',
        'resourceProfileId',
        'resultSerializationTime_sum',
        'resultSize_max',
        'runType',
        'scaleFactor',
        'scan_bw',
        'scan_time',
        'shuffle_read_bw',
        'shuffle_write_bw',
        'sparkVersion',
        'sqlID',
        'sqlOp_AQEShuffleRead',
        'sqlOp_BroadcastExchange',
        'sqlOp_BroadcastHashJoin',
        'sqlOp_BroadcastNestedLoopJoin',
        'sqlOp_CartesianProduct',
        'sqlOp_ColumnarToRow',
        'sqlOp_CustomShuffleReader',
        'sqlOp_DeserializeToObject',
        'sqlOp_Exchange',
        'sqlOp_Execute InsertIntoHadoopFsRelationCommand',
        'sqlOp_Expand',
        'sqlOp_Filter',
        'sqlOp_HashAggregate',
        'sqlOp_LocalTableScan',
        'sqlOp_MapElements',
        'sqlOp_ObjectHashAggregate',
        'sqlOp_Project',
        'sqlOp_RunningWindowFunction',
        'sqlOp_Scan orc ',
        'sqlOp_Scan parquet ',
        'sqlOp_Scan text ',
        'sqlOp_SerializeFromObject',
        'sqlOp_Sort',
        'sqlOp_SortMergeJoin',
        'sqlOp_Subquery',
        'sqlOp_SubqueryBroadcast',
        'sqlOp_TakeOrderedAndProject',
        'sqlOp_Window',
        'sqlOp_WindowGroupLimit',
        'sr_fetchWaitTime_sum',
        'sr_localBlocksFetched_sum',
        'sr_localBytesRead_sum',
        'sr_localBytesReadRatio',
        'sr_remoteBlocksFetched_sum',
        'sr_remoteBytesRead_sum',
        'sr_remoteBytesReadRatio',
        'sr_remoteBytesReadToDisk_sum',
        'sr_remoteBytesReadToDiskRatio',
        'sr_totalBytesRead_sum',
        'sr_totalBytesReadRatio',
        'sw_bytesWritten_sum',
        'sw_bytesWrittenRatio',
        'sw_recordsWritten_sum',
        'sw_writeTime_sum',
        'taskCpu',
        'taskGpu',
    ]
)

expected_model_features = set(
    [
        'cache_hit_ratio',
        'data_size',
        'decode_time',
        'diskBytesSpilled_sum',
        'diskBytesSpilledBool',
        'diskBytesSpilledRatio',
        'duration_max',
        'duration_mean',
        'duration_min',
        'duration_sum',
        'executorCores',
        'executorCPUTime_sum',
        'executorDeserializeCPUTime_sum',
        'executorDeserializeTime_sum',
        'executorMemory',
        'executorOffHeap',
        'executorRunTime_sum',
        'hasSqlID',
        'input_bytesRead_sum',
        'input_bytesReadRatio',
        'input_recordsRead_sum',
        'jvmGCTime_sum',
        'maxMem',
        'maxOffHeapMem',
        'maxOnHeapMem',
        'memoryBytesSpilled_sum',
        'memoryBytesSpilledBool',
        'memoryBytesSpilledRatio',
        'numExecutors',
        'numGpusPerExecutor',
        'numTasks_sum',
        'output_bytesWritten_sum',
        'output_bytesWrittenRatio',
        'output_recordsWritten_sum',
        'peakExecutionMemory_max',
        'resourceProfileId',
        'resultSerializationTime_sum',
        'resultSize_max',
        'scan_bw',
        'scan_time',
        'shuffle_read_bw',
        'shuffle_write_bw',
        'sqlOp_AQEShuffleRead',
        'sqlOp_BroadcastExchange',
        'sqlOp_BroadcastHashJoin',
        'sqlOp_BroadcastNestedLoopJoin',
        'sqlOp_CartesianProduct',
        'sqlOp_ColumnarToRow',
        'sqlOp_CustomShuffleReader',
        'sqlOp_DeserializeToObject',
        'sqlOp_Exchange',
        'sqlOp_Execute InsertIntoHadoopFsRelationCommand',
        'sqlOp_Expand',
        'sqlOp_Filter',
        'sqlOp_HashAggregate',
        'sqlOp_LocalTableScan',
        'sqlOp_MapElements',
        'sqlOp_ObjectHashAggregate',
        'sqlOp_Project',
        'sqlOp_RunningWindowFunction',
        'sqlOp_Scan orc ',
        'sqlOp_Scan parquet ',
        'sqlOp_Scan text ',
        'sqlOp_SerializeFromObject',
        'sqlOp_Sort',
        'sqlOp_SortMergeJoin',
        'sqlOp_Subquery',
        'sqlOp_SubqueryBroadcast',
        'sqlOp_TakeOrderedAndProject',
        'sqlOp_Window',
        'sqlOp_WindowGroupLimit',
        'sr_fetchWaitTime_sum',
        'sr_localBlocksFetched_sum',
        'sr_localBytesRead_sum',
        'sr_localBytesReadRatio',
        'sr_remoteBlocksFetched_sum',
        'sr_remoteBytesRead_sum',
        'sr_remoteBytesReadRatio',
        'sr_remoteBytesReadToDisk_sum',
        'sr_remoteBytesReadToDiskRatio',
        'sr_totalBytesRead_sum',
        'sr_totalBytesReadRatio',
        'sw_bytesWritten_sum',
        'sw_bytesWrittenRatio',
        'sw_recordsWritten_sum',
        'sw_writeTime_sum',
        'taskCpu',
        'taskGpu',
    ]
)


@dataclass
class RegexPattern:
    app_id = re.compile(r'^app.*[_-][0-9]+[_-][0-9]+$')
    profile = re.compile(r'^prof_[0-9]+_[0-9a-zA-Z]+$')
    qual_tool = re.compile(r'^qual_[0-9]+_[0-9a-zA-Z]+$')
    rapids_profile = re.compile(r'rapids_4_spark_profile')
    rapids_qual_tool = re.compile(r'rapids_4_spark_qualification_output')


def find_paths(folder, filter_fn=None, return_directories=False):
    """Find all files or subdirectories in a directory that match a filter function.

    Parameters
    ----------
    folder: str
        Path to directory to search.
    filter_fn: Callable
        Filter function that selects files/directories.
    return_directories: bool
        If true, returns matching directories, otherwise returns matching files
    """
    paths = []
    if folder and os.path.isdir(folder):
        for root, dirs, files in os.walk(folder):
            try:
                if return_directories:
                    filtered_dirs = filter(filter_fn, dirs)
                    paths.extend([os.path.join(root, dir) for dir in filtered_dirs])
                else:
                    filtered_files = filter(filter_fn, files)
                    paths.extend([os.path.join(root, file) for file in filtered_files])
            except Exception as e:  # pylint: disable=broad-except
                logger.error('Error occurred when searching in %s: %s', folder, e)
    return paths


def load_qual_csv(
        qual_dirs: List[str], csv_filename: str, cols: Optional[List[str]] = None
) -> Optional[pd.DataFrame]:
    """
    Load CSV file from qual tool output as pandas DataFrame.
    """
    qual_csv = [os.path.join(q, csv_filename) for q in qual_dirs]
    df = None
    if qual_csv:
        try:
            df = pd.concat([pd.read_csv(f) for f in qual_csv])
            if cols:
                df = df[cols]
        except Exception as e:  # pylint: disable=broad-except
            logger.error('Error concatenating qualification output files: %s', e)
    return df


def load_qtool_execs(qtool_execs: List[str]) -> Optional[pd.DataFrame]:
    """
    Load supported stage info from qtool output in a form that can be merged with profiler data
    to aggregate features and durations only over supported stages.
    """
    node_level_supp = None
    if qtool_execs:
        try:
            exec_info = pd.concat([pd.read_csv(f) for f in qtool_execs])
            node_level_supp = exec_info.copy()
            node_level_supp['Exec Is Supported'] = node_level_supp[
                                                       'Exec Is Supported'
                                                   ] | node_level_supp['Action'].apply(
                lambda x: x == 'IgnoreNoPerf')
            node_level_supp = (
                node_level_supp[['App ID', 'SQL ID', 'SQL Node Id', 'Exec Is Supported']]
                .groupby(['App ID', 'SQL ID', 'SQL Node Id'])
                .agg('all')
                .reset_index(level=[0, 1, 2])
            )
        except Exception as e:  # pylint: disable=broad-except
            logger.error('Error loading supported stage info from qualification output: %s', e)
    return node_level_supp


def get_qual_data(qual: Optional[str]):
    if not qual:
        return None, None, None

    # load qual tool execs
    qual_list = find_paths(
        qual, RegexPattern.rapids_qual_tool.match, return_directories=True
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


def safe_load_csv_files(
        toc: pd.DataFrame,
        app_id: str,
        node_level_supp: Optional[pd.DataFrame],
        qual_tool_filter: Optional[str]) -> Dict[str, pd.DataFrame]:
    """
    This wrapper function calls the load_csv_files function, handling any exceptions.
    """
    try:
        return load_csv_files(toc, app_id, node_level_supp, qual_tool_filter)
    except Exception as e:  # pylint: disable=broad-except
        # We should catch this and continue parsing other applications
        logger.error('Error loading csv data for app_id %s: %s', app_id, e)
        return {}


def load_csv_files(
        toc: pd.DataFrame,
        app_id: str,
        node_level_supp: Optional[pd.DataFrame],
        qual_tool_filter: Optional[str]) -> Dict[str, pd.DataFrame]:
    """
    Load profiler CSV files into memory.
    """

    def scan_tbl(
            tb_name: str, abort_on_error: bool = False, warn_on_error: bool = True
    ) -> pd.DataFrame:
        try:
            out_df = pd.read_csv(
                sgl_app[sgl_app['table_name'] == tb_name]['filepath'].iloc[0]
            )
        except Exception as ex:  # pylint: disable=broad-except
            if warn_on_error or abort_on_error:
                logger.warning('Failed to load %s for %s.', tb_name, app_id)
            if abort_on_error:
                raise ScanTblError() from ex
            out_df = pd.DataFrame()
        return out_df

    # Merge summary tables within each appId:
    sgl_app = toc[toc['appId'] == app_id]

    # Load summary tables:
    app_info = scan_tbl('application_information')

    # Allow user-provided 'test_name' as 'appName'
    # appName = app_info['appName'].iloc[0]
    app_name = sgl_app['test_name'].iloc[0]

    if not app_info.empty:
        app_info['appName'] = app_name
        app_info['sparkVersion'].fillna('Unknown', inplace=True)

    # Get jar versions:
    cudf_version = '-'
    rapids_spark_version = '-'
    bm_runner_version = '-'

    jars_tbl = scan_tbl('rapids_accelerator_jar_and_cudf_jar', warn_on_error=False)
    if not jars_tbl.empty:
        jars_list = list(jars_tbl['Rapids4Spark jars'].str.split('/').str[-1].str[:-4])

        # Parse cudfVersion, rapids4sparkVersion, bmRunnerVersion:
        for jar in jars_list:
            if jar.startswith('cudf'):
                cudf_version = jar

            if jar.startswith('rapids-4-spark_'):
                rapids_spark_version = jar

            if jar.startswith('rapids-4-spark-benchmarks_'):
                bm_runner_version = jar

    if not app_info.empty:
        app_info['cudfVersion'] = cudf_version
        app_info['rapids4sparkVersion'] = rapids_spark_version
        app_info['bmRunnerVersion'] = bm_runner_version

    spark_props = scan_tbl('spark_properties')
    if not spark_props.empty:
        spark_props = spark_props.set_index('propertyName')

    exec_info = scan_tbl('executor_information')

    sql_duration = scan_tbl('sql_duration_and_executor_cpu_time_percent')
    if not sql_duration.empty:
        sql_duration = sql_duration.rename(
            {
                'App Duration': 'appDuration',
                'Contains Dataset or RDD Op': 'containsDatasetOrRDDOp',
            },
            axis=1,
        )
        sql_duration['potentialProblems'] = sql_duration['Potential Problems'] == 'UDF'
        sql_duration = sql_duration.drop(columns=['Potential Problems'])

    sql_app_metrics = scan_tbl('sql_level_aggregated_task_metrics')
    if not sql_app_metrics.empty:
        sql_app_metrics = sql_app_metrics.drop(columns='appIndex')

    # filter out sql ids that have no execs associated with them
    # this should remove root sql ids in 3.4.1+
    sql_to_stage = scan_tbl('sql_to_stage_information')
    if not sql_to_stage.empty:
        # sql_to_stage['SQL Nodes(IDs)']
        # try:
        sqls_with_execs = (
            sql_to_stage.loc[sql_to_stage['SQL Nodes(IDs)'].notna()][['sqlID', 'jobID']]
            .groupby(['sqlID'])
            .first()
            .reset_index()
        )
    else:
        sqls_with_execs = pd.DataFrame()

    if not sql_app_metrics.empty and not sqls_with_execs.empty:
        sql_app_metrics = (
            sql_app_metrics.merge(sqls_with_execs, on='sqlID')
            .drop(columns=['jobID'])
            .reset_index()
        )

    # Job to stageIds/sqlID mapping:
    job_map_tbl = scan_tbl('job_information')
    if not job_map_tbl.empty:
        job_map_tbl = job_map_tbl.rename(columns={'startTime': 'jobStartTime_min'})
        job_map_tbl['sqlID'] = job_map_tbl['sqlID'].fillna(-1).astype(int)
        job_map_tbl['jobID'] = 'job_' + job_map_tbl['jobID'].astype(str)

    # Update sql_plan_metrics_for_application table:
    sql_ops_metrics = scan_tbl('sql_plan_metrics_for_application')
    if not sql_ops_metrics.empty and not app_info.empty:
        sql_ops_metrics = sql_ops_metrics.drop(columns='appIndex')
        sql_ops_metrics['appId'] = app_info['appId'].iloc[0].strip()
        sql_ops_metrics['appName'] = app_name
        if node_level_supp is not None:
            if qual_tool_filter == 'stage':
                sql_ops_metrics = sql_ops_metrics.merge(
                    node_level_supp,
                    left_on=['appId', 'sqlID', 'nodeID'],
                    right_on=['App ID', 'SQL ID', 'SQL Node Id'],
                    how='inner',
                )
                sql_ops_metrics = sql_ops_metrics.drop(
                    columns=['App ID', 'SQL ID', 'SQL Node Id']
                )
                sql_ops_metrics['stageIds'] = sql_ops_metrics['stageIds'].apply(
                    lambda x: str(x).split(',')
                )
                sql_ops_metrics = sql_ops_metrics.explode('stageIds')
                # compute supported stages for use below. TBD.  rename column from
                # 'Exec Is Supported' to 'Stage Is Supported' and change elsewhere
                stages_supp = (
                    sql_ops_metrics.groupby(['appId', 'sqlID', 'stageIds'])[
                        'Exec Is Supported'
                    ]
                    .agg('all')
                    .reset_index()
                )
                stages_supp = stages_supp.loc[
                    stages_supp['stageIds'].apply(lambda x: str(x) != 'nan')
                ].reset_index(drop=True)
                stages_supp['appId'] = stages_supp['appId'].astype(str)
                stages_supp['stageIds'] = (
                    stages_supp['stageIds'].astype(float).astype(int)
                )
                # filter sql ops to have only supported ones for processing in calling fn
                sql_ops_metrics = sql_ops_metrics.loc[
                    sql_ops_metrics['Exec Is Supported']
                ].drop(columns=['Exec Is Supported'])
            else:  # qualtool_filter_by = 'sqlId'
                sql_level_supp = (
                    node_level_supp.groupby(['App ID', 'SQL ID'])['Exec Is Supported']
                    .agg('all')
                    .reset_index()
                )
                sql_level_supp = sql_level_supp.loc[sql_level_supp['Exec Is Supported']]
                sql_app_metrics = sql_app_metrics.merge(
                    sql_level_supp,
                    left_on=['appID', 'sqlID'],
                    right_on=['App ID', 'SQL ID'],
                    how='inner',
                )
                sql_app_metrics = sql_app_metrics.drop(
                    columns=['Exec Is Supported', 'App ID', 'SQL ID']
                )
    else:
        if node_level_supp is not None:
            stages_supp = pd.DataFrame(columns=['appId', 'sqlID', 'stageIds'])
            sql_level_supp = pd.DataFrame(columns=['App ID', 'SQL ID'])

    # sqlids to drop due to failures meeting below criteria
    sqls_to_drop = set()

    # Load job+stage level agg metrics:
    job_stage_agg_tbl = scan_tbl('job_+_stage_level_aggregated_task_metrics')
    if not any([job_stage_agg_tbl.empty, job_map_tbl.empty]):
        job_stage_agg_tbl = job_stage_agg_tbl.drop(columns='appIndex')
        job_stage_agg_tbl = job_stage_agg_tbl.rename(
            columns={'numTasks': 'numTasks_sum', 'duration_avg': 'duration_mean'}
        )
        job_stage_agg_tbl['appId'] = app_info['appId'].iloc[0]
        job_stage_agg_tbl['appName'] = app_name

        # Only need job level aggs for now since one-to-many relationships between stageID and sqlID.
        job_stage_agg_tbl['js_type'] = job_stage_agg_tbl['ID'].str.split('_').str[0]
        # mark for removal from modeling sqlids that have failed stage time > 10% total stage time
        allowed_failed_duration_fraction = 0.10
        stage_agg_tbl = job_stage_agg_tbl.loc[
            job_stage_agg_tbl.js_type == 'stage'
            ].reset_index()
        stage_agg_tbl['ID'] = stage_agg_tbl['ID'].str.split('_').str[1].astype(int)
        failed_stages = scan_tbl('failed_stages', warn_on_error=False)
        if not sql_to_stage.empty and not failed_stages.empty:
            stage_agg_tbl = stage_agg_tbl[['ID', 'Duration']].merge(
                sql_to_stage, left_on='ID', right_on='stageId'
            )
            total_stage_time = (
                stage_agg_tbl[['sqlID', 'ID', 'Duration']]
                .groupby('sqlID')
                .agg('sum')
                .reset_index()
            )
            failed_stage_time = (
                stage_agg_tbl[['sqlID', 'ID', 'Duration']]
                .merge(failed_stages, left_on='ID', right_on='stageId', how='inner')
                .groupby('sqlID')
                .agg('sum')
                .reset_index()
                .drop(columns=['sqlID'])
            )
            stage_times = total_stage_time.merge(
                failed_stage_time, on='ID', how='inner'
            )
            stage_times.info()
            sqls_to_drop = set(
                stage_times.loc[
                    stage_times.Duration_y
                    > stage_times.Duration_x * allowed_failed_duration_fraction
                    ]['sqlID']
            )

        if sqls_to_drop:
            logger.warning('Ignoring sqlIDs %s due to excessive failed/cancelled stage duration.', sqls_to_drop)

        if node_level_supp is not None and (qual_tool_filter == 'stage'):
            job_stage_agg_tbl = job_stage_agg_tbl[
                job_stage_agg_tbl['js_type'] == 'stage'
                ]
            job_stage_agg_tbl['ID'] = (
                job_stage_agg_tbl['ID']
                .apply(lambda id: int(id.split('_')[1]))
                .astype(int)
            )
            job_stage_agg_tbl['appId'] = job_stage_agg_tbl['appId'].astype(str)
            # add per stage 'Exec Is Supported' column and also sqlID (stages_supp has this latter info as well)
            job_stage_agg_tbl = job_stage_agg_tbl.merge(
                stages_supp,
                left_on=['appId', 'ID'],
                right_on=['appId', 'stageIds'],
                how='inner',
            )
            job_stage_agg_tbl = job_stage_agg_tbl.drop(columns=['stageIds'])
        else:
            job_stage_agg_tbl = job_stage_agg_tbl[job_stage_agg_tbl['js_type'] == 'job']

            # Update job+stage agg table:
            job_stage_agg_tbl = job_stage_agg_tbl.merge(
                job_map_tbl[['jobID', 'sqlID', 'jobStartTime_min']],
                left_on='ID',
                right_on='jobID',
                how='left',
            )

        job_stage_agg_tbl['sqlID'] = job_stage_agg_tbl['sqlID'].astype(int)
        job_stage_agg_tbl['hasSqlID'] = job_stage_agg_tbl['sqlID'] != -1
        job_stage_agg_tbl = job_stage_agg_tbl.drop(columns=['ID', 'js_type'])

    # Load wholestage operator info:

    wholestage_tbl = scan_tbl('wholestagecodegen_mapping', warn_on_error=False)
    if not wholestage_tbl.empty:
        wholestage_tbl['appId'] = app_info['appId'].iloc[0]

    # Merge certain tables:
    if not any(
            [app_info.empty, exec_info.empty, sql_app_metrics.empty, sql_duration.empty]
    ):
        app_info_mg = app_info.merge(exec_info, on='appIndex')
        app_info_mg = app_info_mg.merge(
            sql_app_metrics, left_on='appId', right_on='appID'
        )
        app_info_mg = app_info_mg.merge(
            sql_duration[
                [
                    'App ID',
                    'sqlID',
                    'appDuration',
                ]
            ],
            left_on=['appId', 'sqlID'],
            right_on=['App ID', 'sqlID'],
        )
        app_info_mg = app_info_mg.drop(columns=['appID', 'appIndex', 'App ID'])

        # filter out sqlIDs with aborted jobs (these are jobs failed due to sufficiently many (configurable)
        # failed attempts of a stage due to error conditions).
        # these are failed sqlIDs that we shouldn't model, but are still included in profiler output.
        failed_jobs = scan_tbl('failed_jobs', warn_on_error=False)
        if not failed_jobs.empty and not job_map_tbl.empty:
            aborted_jobs = failed_jobs.loc[
                failed_jobs.failureReason.str.contains('aborted')
            ][['jobID']]
            aborted_jobs['jobID'] = 'job_' + aborted_jobs['jobID'].astype(str)
            aborted_jobs_sql_id = job_map_tbl.merge(
                aborted_jobs, how='inner', on='jobID'
            )
            aborted_sql_ids = set(aborted_jobs_sql_id['sqlID'])
        else:
            aborted_sql_ids = set()

        if aborted_sql_ids:
            logger.warning('Ignoring sqlIDs %s due to aborted jobs.', aborted_sql_ids)

        sqls_to_drop = sqls_to_drop.union(aborted_sql_ids)

        if sqls_to_drop:
            logger.warning('Ignoring a total of %d sqlIDs due to stage/job failures.', len(sqls_to_drop))
            app_info_mg = app_info_mg.loc[~app_info_mg.sqlID.isin(sqls_to_drop)]

    else:
        app_info_mg = pd.DataFrame()

    ds_tbl = scan_tbl('data_source_information')

    out = {
        'app_tbl': app_info_mg,
        'ops_tbl': sql_ops_metrics,
        'spark_props_tbl': spark_props,
        'job_map_tbl': job_map_tbl,
        'job_stage_agg_tbl': job_stage_agg_tbl,
        'wholestage_tbl': wholestage_tbl,
        'ds_tbl': ds_tbl,
    }

    return out


def extract_raw_features(
        toc: pd.DataFrame,
        node_level_supp: Optional[pd.DataFrame],
        qualtool_filter: Optional[str],
) -> pd.DataFrame:
    """Given a pandas dataframe of CSV files, extract raw features into a single dataframe keyed by (appId, sqlID)."""
    # read all tables per appId
    unique_app_ids = toc['appId'].unique()
    app_id_tables = [
        safe_load_csv_files(toc, app_id, node_level_supp, qualtool_filter)
        for app_id in unique_app_ids
    ]

    def combine_tables(table_name: str) -> pd.DataFrame:
        """Combine csv tables (by name) across all appIds."""
        merged = pd.concat(
            [app_id_tables[i][table_name] for i in range(len(app_id_tables)) if table_name in app_id_tables[i]]
        )
        return merged

    app_tbl = combine_tables('app_tbl').sort_values('startTime', ascending=True)
    ops_tbl = combine_tables('ops_tbl')
    # job_map_tbl = combine_tables('job_map_tbl')
    job_stage_agg_tbl = combine_tables('job_stage_agg_tbl')
    wholestage_tbl = combine_tables('wholestage_tbl')
    if any([app_tbl.empty, ops_tbl.empty, job_stage_agg_tbl.empty]):
        logger.warning('Empty features tables')
        return pd.DataFrame()

    # normalize dtypes
    app_int_dtypes = ['taskCpu', 'taskGpu']
    app_tbl[app_int_dtypes] = app_tbl[app_int_dtypes].fillna(0).astype(int)

    # normalize timings from ns to ms
    ns_timing_mask = ops_tbl['metricType'] == 'nsTiming'
    ops_tbl.loc[ns_timing_mask, 'max'] = (
            ops_tbl.loc[ns_timing_mask, 'max'] / 1e6
    ).astype(np.int64)
    ops_tbl.loc[ops_tbl['metricType'] == 'nsTiming', 'metricType'] = 'timing'

    # normalize WholeStageCodegen labels
    ops_tbl.loc[
        ops_tbl['nodeName'].str.startswith('WholeStageCodegen'), 'nodeName'
    ] = 'WholeStageCodegen'

    # format WholeStageCodegen for merging
    try:
        wholestage_tbl_filter = wholestage_tbl[
            ['appId', 'sqlID', 'nodeID', 'Child Node']
        ].rename(columns={'Child Node': 'nodeName'})
    except Exception:  # pylint: disable=broad-except
        wholestage_tbl_filter = pd.DataFrame()

    # remove WholeStageCodegen from original ops table and replace with constituent ops
    ops_tbl_filter = ops_tbl[ops_tbl['nodeName'] != 'WholeStageCodegen']
    ops_tbl_filter = (
        ops_tbl_filter.groupby(['appId', 'sqlID', 'nodeID'])['nodeName']
        .first()
        .reset_index()
    )
    sql_ops_counter = pd.concat([ops_tbl_filter, wholestage_tbl_filter])
    sql_ops_counter['counter'] = 1  # counter col for pivot_table()
    sql_ops_list = list(sql_ops_counter['nodeName'].unique())  # unique sql ops
    sql_ops_map = {cc: 'sqlOp_' + cc for cc in sql_ops_list}  # add prefix to sql ops
    sql_ops_counter['nodeName'] = sql_ops_counter['nodeName'].map(sql_ops_map)
    sql_ops_list = list(
        sql_ops_counter['nodeName'].unique()
    )  # update unique sql ops list

    # pivot sql ops rows to columns
    sql_ops_counter = (
        pd.pivot_table(
            sql_ops_counter,
            index=['appId', 'sqlID'],
            values='counter',
            columns='nodeName',
        )
        .fillna(0)
        .astype(int)
        .reset_index()
    )

    # identify reduce ops using suffix of column names
    job_stage_reduce_cols = {
        cc: cc.split('_')[-1]
        for cc in job_stage_agg_tbl.columns
        if cc.split('_')[-1] in ['sum', 'min', 'max', 'mean']
    }

    if node_level_supp is not None and (qualtool_filter == 'stage'):
        # if supported exec info supplied aggregate features only over supported stages
        sql_job_agg_tbl = job_stage_agg_tbl.loc[job_stage_agg_tbl['Exec Is Supported']]
        if sql_job_agg_tbl.empty:
            logger.warning('No fully supported stages.')
            return pd.DataFrame()
    else:
        sql_job_agg_tbl = job_stage_agg_tbl

    # aggregate using reduce ops, recomputing duration_mean
    sql_job_agg_tbl = job_stage_agg_tbl.groupby(
        ['appId', 'appName', 'sqlID'], as_index=False
    ).agg(job_stage_reduce_cols)
    sql_job_agg_tbl['duration_mean'] = (
            sql_job_agg_tbl['duration_sum'] / sql_job_agg_tbl['numTasks_sum']
    )

    sql_job_agg_tbl = sql_job_agg_tbl.merge(
        sql_ops_counter, on=['appId', 'sqlID'], how='left'
    )

    # merge app attributes
    app_cols = [
        'appId',
        'appDuration',
        'sqlID',
        'Duration',
        'description',
        'sparkVersion',
        'pluginEnabled',
        'resourceProfileId',
        'numExecutors',
        'executorCores',
        'maxMem',
        'maxOnHeapMem',
        'maxOffHeapMem',
        'executorMemory',
        'numGpusPerExecutor',
        'executorOffHeap',
        'taskCpu',
        'taskGpu',
    ]

    sql_job_agg_tbl['appId'] = sql_job_agg_tbl['appId'].str.strip()
    app_tbl['appId'] = app_tbl['appId'].str.strip()

    sql_job_agg_tbl = sql_job_agg_tbl.merge(
        app_tbl[app_cols], on=['appId', 'sqlID'], how='inner'
    )

    # filter rows w/ sqlID
    sql_job_agg_tbl['hasSqlID'] = sql_job_agg_tbl['sqlID'] > 0
    full_tbl = sql_job_agg_tbl[sql_job_agg_tbl['hasSqlID']]

    # add runType features from toc
    app_runtype = toc[['appId', 'runType']].drop_duplicates()
    full_tbl = full_tbl.merge(app_runtype, on='appId')

    # impute missing ops and fix dtype
    full_tbl[sql_ops_list] = full_tbl[sql_ops_list].fillna(0).astype(int)
    full_tbl = full_tbl.copy()  # defragment dataframe

    # correct input_bytesRead based on cache hit ratio for Databricks
    cache_info = ops_tbl.loc[
        ((ops_tbl.name == 'cache hits size') | (ops_tbl.name == 'cache misses size'))
    ]
    if not cache_info.empty:
        cache_ratio = (
            cache_info[['appId', 'sqlID', 'name', 'total']]
            .set_index(['appId', 'sqlID', 'name'])
            .groupby(['appId', 'sqlID', 'name'])
            .agg('sum')
        )
        cache_ratio = cache_ratio.reset_index().pivot(
            index=['appId', 'sqlID'], columns=['name'], values=['total']
        )
        cache_ratio.columns = cache_ratio.columns.droplevel().values
        cache_ratio['cache_hit_ratio'] = cache_ratio['cache hits size'] / (
                cache_ratio['cache hits size'] + cache_ratio['cache misses size']
        )
        cache_ratio['input_bytesRead_cache'] = (
                cache_ratio['cache hits size'] + cache_ratio['cache misses size']
        )
        cache_ratio = cache_ratio.drop(columns=['cache hits size', 'cache misses size'])
        full_tbl = full_tbl.merge(cache_ratio, on=['appId', 'sqlID'], how='left')
        full_tbl['input_bytesRead_sum'] = full_tbl[
            ['input_bytesRead_sum', 'input_bytesRead_cache']
        ].max(axis=1)
        full_tbl = full_tbl.drop(columns=['input_bytesRead_cache'])
        full_tbl['cache_hit_ratio'].fillna(0.0, inplace=True)
    else:
        full_tbl['cache_hit_ratio'] = 0.0

    if node_level_supp is not None and (qualtool_filter == 'stage'):
        # if supported info supplied and filtering by supported stage
        # add a column with fraction of total task time supported for each sql ID
        time_ratio = (
            job_stage_agg_tbl[['appId', 'sqlID', 'Exec Is Supported', 'duration_sum']]
            .set_index(['appId', 'sqlID', 'Exec Is Supported'])
            .groupby(['appId', 'sqlID', 'Exec Is Supported'])
            .agg('sum')
        )
        time_ratio = time_ratio.reset_index().pivot(
            index=['appId', 'sqlID'],
            columns=['Exec Is Supported'],
            values=['duration_sum'],
        )
        time_ratio.columns = time_ratio.columns.droplevel().values
        time_ratio = time_ratio.reset_index().fillna(0)
        if True not in time_ratio.columns:
            time_ratio[True] = 0.0
        if False not in time_ratio.columns:
            time_ratio[False] = 0.0
        time_ratio['fraction_supported'] = time_ratio[True] / (
                time_ratio[False] + time_ratio[True]
        )
        time_ratio = time_ratio.drop(columns=[True, False])
        full_tbl = full_tbl.merge(time_ratio, on=['appId', 'sqlID'], how='inner')

    # add byte ratio features
    byte_ratio_features = [
        'diskBytesSpilled',
        'memoryBytesSpilled',
        'input_bytesRead',
        'output_bytesWritten',
        'sr_localBytesRead',
        'sr_remoteBytesRead',
        'sr_remoteBytesReadToDisk',
        'sr_totalBytesRead',
        'sw_bytesWritten',
    ]

    for cc in byte_ratio_features:
        full_tbl[cc + 'Ratio'] = full_tbl[cc + '_sum'] / full_tbl['input_bytesRead_sum']
        full_tbl[cc + 'Ratio'] = full_tbl[cc + 'Ratio'].replace([np.inf, -np.inf], 0)
        full_tbl[cc + 'Ratio'] = full_tbl[cc + 'Ratio'].fillna(0)

    # add binary features
    binarize_features = ['diskBytesSpilled', 'memoryBytesSpilled']

    for cc in binarize_features:
        full_tbl[cc + 'Bool'] = full_tbl[cc + '_sum'] > 0

    # add data source features
    ds_tbl = combine_tables('ds_tbl')
    grouped_ds_tbl = ds_tbl.groupby(['sqlID'], as_index=False).sum()
    grouped_ds_tbl['scan_bw'] = (
            1.0 * grouped_ds_tbl['data_size'] / grouped_ds_tbl['scan_time']
    )
    ds_cols = [
        'sqlID',
        'scan_bw',
        'scan_time',
        'decode_time',
        'data_size',
    ]
    full_tbl = full_tbl.merge(grouped_ds_tbl[ds_cols], on=['sqlID'], how='left')

    # add shuffle bandwidth aggregate features
    full_tbl['shuffle_read_bw'] = (
        (full_tbl['sr_totalBytesRead_sum'] / full_tbl['sr_fetchWaitTime_sum'])
        .replace([np.inf, -np.inf], 0)
        .fillna(0)
    )
    full_tbl['shuffle_write_bw'] = (
        (full_tbl['sw_bytesWritten_sum'] / full_tbl['sw_writeTime_sum'])
        .replace([np.inf, -np.inf], 0)
        .fillna(0)
    )
    full_tbl[ds_cols] = full_tbl[ds_cols].replace([np.inf, -np.inf], 0).fillna(0)

    return full_tbl


def impute(full_tbl: pd.DataFrame) -> pd.DataFrame:
    """Impute missing columns and delete extra columns."""
    actual_features = set(full_tbl.columns)
    if actual_features == expected_raw_features:
        logger.info('Dataset has all expected features')
    else:
        missing = sorted(expected_raw_features - actual_features)
        extra = sorted(actual_features - expected_raw_features)
        if missing:
            logger.warning('Imputing missing features: %s', missing)
            for col in missing:
                if col != 'fraction_supported':
                    full_tbl[col] = 0
                else:
                    full_tbl[col] = 1.0

        if extra:
            logger.warning('Removing extra features: %s', extra)
            full_tbl = full_tbl.drop(columns=extra)

        # one last check after modifications (update expected_raw_features if needed)
        assert set(full_tbl.columns) == expected_raw_features

    return full_tbl


def load_profiles(
        profiles: Mapping[str, Mapping],
        node_level_supp: Optional[pd.DataFrame] = None,
        qualtool_filter: Optional[str] = None,
) -> pd.DataFrame:
    all_raw_features = []
    # get list of csv files from each profile
    for dataset_name, dataset_meta in profiles.items():
        toc_list = []
        profile_paths = dataset_meta.get('profiles', [])
        # use default values for prediction if no app_meta provided
        app_meta = dataset_meta.get(
            'app_meta', {'default': {'runType': 'CPU', 'scaleFactor': 1}}
        )
        scalefactor_meta = dataset_meta.get('scaleFactorFromSqlIDRank', None)
        for path in profile_paths:
            for app_id in app_meta.keys():
                try:
                    if app_id == 'default':
                        csv_files = glob.glob(f'{path}/**/*.csv', recursive=True)
                    else:
                        csv_files = glob.glob(f'{path}/**/{app_id}/*.csv', recursive=True)
                    if csv_files:
                        tmp = pd.DataFrame({'filepath': csv_files})
                        fp_split = tmp['filepath'].str.split(r'/')
                        tmp['test_name'] = dataset_name
                        tmp['appId'] = fp_split.str[-2]
                        tmp['table_name'] = fp_split.str[-1].str[:-4]
                        tmp['runType'] = (
                            app_meta[app_id]['runType']
                            if app_id in app_meta
                            else app_meta['default']['runType']
                        )
                        if not scalefactor_meta:
                            tmp['scaleFactor'] = (
                                app_meta[app_id]['scaleFactor']
                                if app_id in app_meta
                                else app_meta['default']['scaleFactor']
                            )
                        toc_list.append(tmp)
                except Exception as e:  # pylint: disable=broad-except
                    # We should catch this and continue parsing other applications
                    logger.error('Error parsing application %s: %s', app_id, e)

        if toc_list:
            toc = pd.concat(toc_list)
            raw_features = extract_raw_features(toc, node_level_supp, qualtool_filter)
            if raw_features.empty:
                continue
            # add scaleFactor from toc or from sqlID ordering within queries grouped by query name and app
            if scalefactor_meta:
                raw_features['scaleFactorIndex'] = (
                    raw_features.groupby(['description', 'appId'])['sqlID']
                    .rank()
                    .astype('int')
                )
                raw_features['scaleFactor'] = (
                    raw_features['scaleFactorIndex']
                    .astype('string')
                    .map(scalefactor_meta)
                )
                raw_features = raw_features.drop(columns=['scaleFactorIndex'])
            else:
                app_scales = toc[['appId', 'scaleFactor']].drop_duplicates()
                raw_features = raw_features.merge(app_scales, on='appId')
            raw_features = impute(raw_features)
            all_raw_features.append(raw_features)
    return (
        pd.concat(all_raw_features).reset_index(drop=True)
        if all_raw_features
        else pd.DataFrame()
    )


def _get_model(platform: str) -> xgb.Booster:
    model_path = Utils.resource_path(f'qualx/models/xgboost/{platform}.json')
    xgb_model = xgb.Booster()
    xgb_model.load_model(model_path)
    return xgb_model


def extract_model_features(
        df: pd.DataFrame, split_fn: Callable[[pd.DataFrame], pd.DataFrame] = None
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
            logger.warning('Number of GPU rows (%s) does not match number of CPU rows (%s)',
                           gpu_aug_tbl.shape[0], cpu_aug_tbl.shape[0])
        # train/validation dataset with CPU + GPU runs
        gpu_aug_tbl = gpu_aug_tbl[
            ['appName', 'scaleFactor', 'sqlID', 'Duration', 'description']
        ]
        gpu_aug_tbl = gpu_aug_tbl.rename(columns={'Duration': 'xgpu_Duration'})
        cpu_aug_tbl = cpu_aug_tbl.merge(
            gpu_aug_tbl,
            on=['appName', 'scaleFactor', 'sqlID', 'description'],
            how='left'
        )

        # calculate Duration_speedup
        cpu_aug_tbl['Duration_speedup'] = (
                cpu_aug_tbl['Duration'] / cpu_aug_tbl['xgpu_Duration']
        )
        cpu_aug_tbl = cpu_aug_tbl.drop(columns=['xgpu_Duration'])

        # use Duration_speedup as label
        label_col = 'Duration_speedup'

        # remove nan label entries
        original_num_rows = cpu_aug_tbl.shape[0]
        cpu_aug_tbl = cpu_aug_tbl.loc[~cpu_aug_tbl[label_col].isna()]
        if cpu_aug_tbl.shape[0] < original_num_rows:
            logger.warning(
                'Removed %d rows with NaN label values', original_num_rows - cpu_aug_tbl.shape[0])
    else:
        # inference dataset with CPU runs only
        label_col = None

    # non-training features (and labels)
    ignore_cols = [
        'AQE',
        'Duration_speedup',
        'Duration',
        'appId',
        'appDuration',
        'appName',
        'cfgId',
        'completedQueryFlag',
        'decimal',
        'description',
        'fraction_supported',
        'gpu_queryTimeRaw',
        'jobID',
        'jobStartTime_min',
        'Network',
        'pluginEnabled',
        'queryStatus',
        'queryTimeRaw',
        'runType',
        'scaleFactor',
        'sparkVersion',
        'sqlID',
        'Storage',
        'testId',
        'testName',
        'testSet',
        'UCX'
    ]

    # remove non-training columns
    feature_cols = [cc for cc in cpu_aug_tbl.columns if cc not in ignore_cols]

    # sanity check features
    actual = set(feature_cols)
    missing = expected_model_features - actual
    extra = actual - expected_model_features
    if missing:
        raise ValueError(f'Input data is missing model features: {missing}')
    if extra:
        logger.warning('Input data has extra features: %s', extra)

    # add train/val/test split column, if split function provided
    if split_fn:
        cpu_aug_tbl = split_fn(cpu_aug_tbl)

    return cpu_aug_tbl, feature_cols, label_col


def predict_model(
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
        logger.warning('Input had extra features not present in model: %s', extra)

    x_dim = cpu_aug_tbl[model_features]
    y_dim = cpu_aug_tbl[label_col] if label_col else None

    dmat = xgb.DMatrix(x_dim, y_dim)
    y_pred = xgb_model.predict(dmat)

    # shapley explainer for prediction
    pd.set_option('display.max_rows', None)
    explainer = shap.TreeExplainer(xgb_model)
    shap_values = explainer.shap_values(x_dim)
    shap_vals = np.abs(shap_values).mean(axis=0)
    feature_importance = pd.DataFrame(
            list(zip(feature_cols, shap_vals)), columns=['feature', 'shap_value']
    )
    feature_importance.sort_values(by=['shap_value'], ascending=False, inplace=True)
    shap_values_path = output_info['shapValues']['path']
    logger.info('Writing SHAPley values to: %s', shap_values_path)
    feature_importance.to_csv(shap_values_path, index=False)
    logger.info('Feature importance (SHAPley values)\n %s', feature_importance)

    if y_dim is not None:
        # evaluation
        if LOG_LABEL:
            y_pred = np.exp(y_pred)

        preds = {'y': y_dim, 'y_pred': y_pred}
        preds_df = pd.DataFrame(preds)

        # add absolute and percentage errors
        preds_df['abs_error'] = np.abs(preds_df['y_pred'] - preds_df['y'])
        preds_df['pct_error'] = 100 * preds_df['abs_error'] / preds_df['y']
    else:
        # inference
        if LOG_LABEL:
            y_pred = np.exp(y_pred)

        preds = {'y_pred': y_pred}
        preds_df = pd.DataFrame(preds)

    select_columns = [
        'appName',
        'appId',
        'appDuration',
        'sqlID',
        'scaleFactor',
        'Duration',
        'fraction_supported',
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
        results_df['gpuDuration'] = results_df['gpuDuration'].astype('long')

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
    summary[cols] = summary[cols].astype('long')
    return summary


def _print_summary(summary):
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


def _print_speedup_summary(dataset_summary: pd.DataFrame):
    try:
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
    except Exception as e:  # pylint: disable=broad-except
        logger.error('Error generating summary for stdout: %s', e)


def predict(platform: str = 'onprem',
            qual: Optional[str] = None,
            profile: Optional[str] = None,
            output_info: Optional[dict] = None,
            qualtool_filter: Optional[str] = 'stage') -> pd.DataFrame:
    xgb_model = _get_model(platform)
    node_level_supp, _, _ = get_qual_data(qual)

    # preprocess profiles
    if profile:
        profile_list = find_paths(
            profile,
            RegexPattern.rapids_profile.match,
            return_directories=True,
        )
        # use parent directory of `rapids_4_spark_profile`
        profile_list = [Path(p).parent for p in profile_list]
        processed_dfs = {}
        for prof in profile_list:
            datasets = {}
            # add profiles
            dataset_name = Path(prof).name
            datasets[dataset_name] = {'profiles': [prof], 'app_meta': {}}
            # search profile sub directories for appIds
            app_ids = find_paths(
                prof, RegexPattern.app_id.match, return_directories=True
            )
            app_ids = [Path(p).name for p in app_ids]
            if len(app_ids) == 0:
                logger.warning('Skipping empty profile: %s', prof)
            else:

                try:
                    for app_id in app_ids:
                        # create dummy app_meta, assuming CPU and scale factor of 1 (for inference)
                        datasets[dataset_name]['app_meta'].update(
                            {app_id: {'runType': 'CPU', 'scaleFactor': 1}}
                        )
                    logger.info('Loading dataset %s', dataset_name)
                    profile_df = load_profiles(
                        datasets, node_level_supp, qualtool_filter
                    )
                    processed_dfs[dataset_name] = profile_df
                except ScanTblError:
                    # ignore
                    logger.error('Skipping invalid dataset: %s', dataset_name)
    if not processed_dfs:
        raise ValueError('No profile data found.')
    # predict on each input dataset
    dataset_summaries = []
    for dataset, input_df in processed_dfs.items():
        if not input_df.empty:
            filter_str = (
                f'with {qualtool_filter} filtering'
                if node_level_supp is not None and any(input_df['fraction_supported'] != 1.0)
                else 'raw'
            )
            logger.info('Predicting dataset (%s): %s', filter_str, dataset)
            features, feature_cols, label_col = extract_model_features(input_df)
            # note: dataset name is already stored in the 'appName' field
            try:
                results = predict_model(xgb_model, features, feature_cols, label_col, output_info)

                # compute per-app speedups
                summary = _compute_summary(results)
                dataset_summaries.append(summary)
                if INTERMEDIATE_DATA_ENABLED:
                    _print_summary(summary)

                # compute speedup for the entire dataset
                dataset_speedup = (
                        summary['appDuration'].sum() / summary['appDuration_pred'].sum()
                )
                print(f'Dataset estimated speedup: {dataset_speedup:.2f}')

                # write CSV reports
                sql_predictions_path = output_info['perSql']['path']
                logger.info('Writing per-SQL predictions to: %s', sql_predictions_path)
                results.to_csv(sql_predictions_path, index=False)

                app_predictions_path = output_info['perApp']['path']
                logger.info('Writing per-application predictions to: %s', app_predictions_path)
                summary.to_csv(app_predictions_path, index=False)

            except XGBoostError as e:
                # ignore and continue
                logger.error(e)
            except Exception as e:  # pylint: disable=broad-except
                # ignore and continue
                logger.error(e)
                traceback.print_exc(e)
        else:
            logger.warning('Nothing to predict for dataset %s', dataset)

    if dataset_summaries:
        # show summary stats across all datasets
        dataset_summary = pd.concat(dataset_summaries)
        if INTERMEDIATE_DATA_ENABLED:
            _print_speedup_summary(dataset_summary)
        return dataset_summary
    return pd.DataFrame()
