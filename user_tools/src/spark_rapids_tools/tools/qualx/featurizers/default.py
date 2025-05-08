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

"""Default featurizer for Qualx."""

from typing import Dict, Optional

import numpy as np
import pandas as pd

from spark_rapids_tools.tools.qualx.config import get_label
from spark_rapids_tools.tools.qualx.util import log_fallback, get_logger

logger = get_logger(__name__)


# expected features for the dataframe produced by preprocessing
# comments show the profiler source file (and column name, if different)
# N/A indicates that the feature is derived from other features or other sources
expected_raw_features = {
    'appDuration',  # sql_duration_and_executor_cpu_time_percent (App Duration)
    'appId',  # application_information
    'appName',  # application_information
    'cache_hit_ratio',  # sql_plan_metrics_for_application (cache hits size, cache misses size)
    'data_size',  # data_source_information
    'decode_time',  # data_source_information
    'description',  # data_source_information
    'diskBytesSpilled_mean',  # job_level_aggregated_task_metrics (diskBytesSpilled_sum)
    'diskBytesSpilledRatio',  # N/A (diskBytesSpilled_sum / input_bytesRead_sum)
    'duration_max',  # job_level_aggregated_task_metrics
    'duration_mean',  # job_level_aggregated_task_metrics (duration_avg)
    'duration_min',  # job_level_aggregated_task_metrics
    'duration_ratio',  # N/A (duration_sum / Duration)
    'duration_sum',  # job_level_aggregated_task_metrics
    'Duration',  # job_level_aggregated_task_metrics
    'executorCores',  # executor_information
    'executorCPUTime_mean',  # job_level_aggregated_task_metrics (executorCpuTime_sum)
    'executorDeserializeCPUTime_mean',  # job_level_aggregated_task_metrics (executorDeserializeCpuTime_sum)
    'executorDeserializeTime_mean',  # job_level_aggregated_task_metrics (executorDeserializeTime_sum)
    'executorMemory',  # executor_information
    'executorOffHeap',  # executor_information
    'executorRunTime_mean',  # job_level_aggregated_task_metrics (executorCpuTime_sum)
    'failed_tasks',  # failed_tasks (attempts)
    'failed_tasks_ratio',  # N/A (failed_tasks / numTasks)
    'fraction_supported',  # N/A fraction of stage durations supported by GPU
    'input_bytesRead_mean',  # job_level_aggregated_task_metrics (input_bytesRead_sum)
    'input_bytesReadRatio',  # N/A (1, kept since byte features are generically used to compute _mean features)
    'input_recordsRead_sum',  # job_level_aggregated_task_metrics (input_recordsRead_sum)
    'jvmGCTime_mean',  # job_level_aggregated_task_metrics (jvmGCTime_sum)
    'maxMem',  # executor_information
    'maxOffHeapMem',  # executor_information
    'maxOnHeapMem',  # executor_information
    'memoryBytesSpilled_mean',  # job_level_aggregated_task_metrics (memoryBytesSpilled_sum)
    'memoryBytesSpilledRatio',  # N/A (memoryBytesSpilled_sum / input_bytesRead_sum)
    'numExecutors',  # executor_information
    'numGpusPerExecutor',  # executor_information
    'numTasks_sum',  # job_level_aggregated_task_metrics (numTasks)
    'output_bytesWritten_mean',  # job_level_aggregated_task_metrics (output_bytesWritten_sum)
    'output_bytesWrittenRatio',  # N/A (output_bytesWritten_sum / input_bytesRead_sum)
    'output_recordsWritten_sum',  # job_level_aggregated_task_metrics
    'peakExecutionMemory_max',  # job_level_aggregated_task_metrics
    'platform_databricks-aws',  # N/A
    'platform_databricks-azure',  # N/A
    'platform_dataproc',  # N/A
    'platform_emr',  # N/A
    'platform_onprem',  # N/A
    'pluginEnabled',  # application_information
    'resultSerializationTime_sum',  # job_level_aggregated_task_metrics
    'resultSize_max',  # job_level_aggregated_task_metrics
    'runType',  # N/A (CPU or GPU)
    'scaleFactor',  # N/A (scale factor of training set, use 1 if only one scale factor)
    'scan_bw',  # N/A (data_size / scan_time)
    'scan_time',  # data_source_information
    'shuffle_read_bw',  # N/A (sr_totalBytesRead_sum / sr_fetchWaitTime_sum)
    'shuffle_write_bw',  # N/A (sw_bytesWritten_sum / sw_writeTime_sum)
    'sparkRuntime',  # application_information
    'sparkVersion',  # application_information
    'sqlID',  # job_level_aggregated_task_metrics
    'sqlOp_AQEShuffleRead',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_BatchEvalPython',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_BroadcastExchange',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_BroadcastHashJoin',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_BroadcastNestedLoopJoin',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_CartesianProduct',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_ColumnarToRow',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_CommandResult',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_CustomShuffleReader',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_DeserializeToObject',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_Exchange',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_Execute InsertIntoHadoopFsRelationCommand csv',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_Execute InsertIntoHadoopFsRelationCommand parquet',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_Execute InsertIntoHadoopFsRelationCommand orc',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_Execute InsertIntoHadoopFsRelationCommand json',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_Execute InsertIntoHadoopFsRelationCommand text',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_Execute InsertIntoHadoopFsRelationCommand unknown',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_Expand',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_Filter',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_Generate',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_GenerateBloomFilter',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_GlobalLimit',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_HashAggregate',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_HashAggregatePrefixGroupingSets',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_LocalLimit',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_LocalTableScan',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_MapElements',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_ObjectHashAggregate',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_OutputAdapter',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_PartialWindow',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_Project',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_ReusedSort',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_RunningWindowFunction',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_Scan csv',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_Scan ExistingRDD Delta Table Checkpoint',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_Scan ExistingRDD Delta Table State',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_Scan ExistingRDD',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_Scan jdbc',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_Scan json',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_Scan OneRowRelation',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_Scan orc',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_Scan parquet',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_Scan text',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_Scan unknown',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_SerializeFromObject',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_Sort',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_SortAggregate',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_SortMergeJoin',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_Subquery',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_SubqueryBroadcast',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_SubqueryOutputBroadcast',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_TakeOrderedAndProject',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_Window',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_WindowGroupLimit',  # sql_plan_metrics_for_application (nodeName)
    'sqlOp_WindowSort',  # sql_plan_metrics_for_application (nodeName)
    'sr_fetchWaitTime_mean',  # job_level_aggregated_task_metrics (sr_fetchWaitTime_sum)
    'sr_localBlocksFetched_sum',  # job_level_aggregated_task_metrics
    'sr_localBytesRead_mean',  # job_level_aggregated_task_metrics (sr_localBytesRead_sum)
    'sr_localBytesReadRatio',  # N/A (sr_localBytesRead_sum / input_bytesRead_sum)
    'sr_remoteBlocksFetched_sum',  # job_level_aggregated_task_metrics
    'sr_remoteBytesRead_mean',  # job_level_aggregated_task_metrics (sr_remoteBytesRead_sum)
    'sr_remoteBytesReadRatio',  # N/A (sr_remoteBytesRead_sum / input_bytesRead_sum)
    'sr_remoteBytesReadToDisk_mean',  # job_level_aggregated_task_metrics (sr_remoteBytesReadToDisk_sum)
    'sr_remoteBytesReadToDiskRatio',  # N/A (sr_remoteBytesReadToDisk_sum / input_bytesRead_sum)
    'sr_totalBytesRead_mean',  # job_level_aggregated_task_metrics (sr_totalBytesRead_sum)
    'sr_totalBytesReadRatio',  # N/A (sr_totalBytesRead_sum / input_bytesRead_sum)
    'sw_bytesWritten_mean',  # job_level_aggregated_task_metrics (sw_bytesWritten_sum)
    'sw_bytesWrittenRatio',  # N/A (sw_bytesWritten_sum / input_bytesRead_sum)
    'sw_recordsWritten_sum',  # job_level_aggregated_task_metrics
    'sw_writeTime_mean',  # job_level_aggregated_task_metrics (sw_writeTime_sum)
    'taskCpu',  # executor_information
    'taskGpu',  # executor_information
}


def extract_raw_features(
    toc: pd.DataFrame,
    node_level_supp: Optional[pd.DataFrame],
    qualtool_filter: Optional[str],
    qualtool_output: Optional[pd.DataFrame] = None,
    remove_failed_sql: bool = True,
) -> pd.DataFrame:
    """Given a pandas dataframe of CSV files, extract raw features into a single dataframe keyed by (appId, sqlID).

    Parameters
    ----------
    toc: pd.DataFrame
        Table of contents of CSV files for the dataset.
    node_level_supp: pd.DataFrame
        Node-level support information used to filter out metrics associated with unsupported operators.
    qualtool_filter: str
        Type of filter to apply to the qualification tool output, either 'stage' or None.
    qualtool_output: pd.DataFrame
        Qualification tool output.
    remove_failed_sql: bool
        Remove sqlIDs with high failure rates, default: True.
    """
    # read all tables per appId
    unique_app_ids = toc['appId'].unique()
    app_id_tables = [
        load_csv_files(
            toc,
            app_id,
            node_level_supp=node_level_supp,
            qualtool_filter=qualtool_filter,
            qualtool_output=qualtool_output,
            remove_failed_sql=remove_failed_sql
        )
        for app_id in unique_app_ids
    ]

    def combine_tables(table_name: str) -> pd.DataFrame:
        """Combine csv tables (by name) across all appIds."""
        merged = pd.concat(
            [app_data[table_name] for app_data in app_id_tables if table_name in app_data]
        )
        return merged

    app_tbl = combine_tables('app_tbl').sort_values('startTime', ascending=True)
    ops_tbl = combine_tables('ops_tbl')

    # job_map_tbl = combine_tables('job_map_tbl')
    job_stage_agg_tbl = combine_tables('job_stage_agg_tbl')
    whole_stage_tbl = combine_tables('wholestage_tbl')
    # feature tables that must be non-empty
    features_tables = [
        (app_tbl, 'application_information'),
        (ops_tbl, 'sql_plan_metrics_for_application'),
        (job_stage_agg_tbl, 'job_+_stage_level_aggregated_task_metrics')
    ]
    empty_tables = [tbl_name for tbl, tbl_name in features_tables if tbl.empty]
    if empty_tables:
        empty_tables_str = ', '.join(empty_tables)
        log_fallback(logger, unique_app_ids,
                     fallback_reason=f'Empty feature tables found after preprocessing: {empty_tables_str}')
        return pd.DataFrame(columns=list(expected_raw_features))

    if get_label() == 'duration_sum':
        # override appDuration with sum(duration_sum) across all stages per appId
        app_duration_sum = job_stage_agg_tbl.groupby('appId')['duration_sum'].sum().reset_index()
        app_duration_sum = app_duration_sum.rename(columns={'duration_sum': 'appDuration'})
        app_tbl = app_tbl.merge(app_duration_sum, on=['appId'], how='left', suffixes=['_orig', None])

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
        ops_tbl['nodeName'].astype(str).str.startswith('WholeStageCodegen'), 'nodeName'
    ] = 'WholeStageCodegen'

    # format WholeStageCodegen for merging
    try:
        whole_stage_tbl_filter = whole_stage_tbl[
            ['appId', 'sqlID', 'nodeID', 'Child Node']
        ].rename(columns={'Child Node': 'nodeName'})
    except Exception:  # pylint: disable=broad-except
        whole_stage_tbl_filter = pd.DataFrame()

    # remove WholeStageCodegen from original ops table and replace with constituent ops
    ops_tbl_filter = ops_tbl[ops_tbl['nodeName'] != 'WholeStageCodegen']
    ops_tbl_filter = (
        ops_tbl_filter.groupby(['appId', 'sqlID', 'nodeID'])['nodeName']
        .first()
        .reset_index()
    )
    sql_ops_counter = pd.concat([ops_tbl_filter, whole_stage_tbl_filter])

    # normalize sqlOp labels w/ variable suffixes
    # note: have to do this after unpacking WholeStageCodegen ops
    dynamic_op_labels = [
        # CPU
        'Scan DeltaCDFRelation',
        'Scan ExistingRDD Delta Table Checkpoint',
        'Scan ExistingRDD Delta Table State',
        'Scan jdbc',
        'Scan parquet',
        # GPU
        'GpuScan parquet',
    ]
    for op in dynamic_op_labels:
        sql_ops_counter.loc[
            sql_ops_counter['nodeName'].str.startswith(op), 'nodeName'
        ] = op

    # count occurrences
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
            log_fallback(logger, unique_app_ids, fallback_reason='No fully supported stages found')
            return pd.DataFrame(columns=list(expected_raw_features))

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
        'sparkRuntime',
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
    sql_job_agg_tbl['hasSqlID'] = sql_job_agg_tbl['sqlID'] >= 0
    full_tbl = sql_job_agg_tbl[sql_job_agg_tbl['hasSqlID']]

    # add duration_ratio feature
    full_tbl['duration_ratio'] = full_tbl['duration_sum'] / full_tbl['Duration']
    full_tbl['duration_ratio'] = full_tbl['duration_ratio'].replace([np.inf, -np.inf], 0).fillna(0)

    # add failed tasks features
    failed_tasks_tbl = combine_tables('failed_tasks_tbl')
    full_tbl = full_tbl.merge(failed_tasks_tbl, on=['appName', 'appId', 'sqlID'], how='left')
    full_tbl['failed_tasks_ratio'] = full_tbl['failed_tasks'] / full_tbl['numTasks_sum']
    full_tbl.fillna({'failed_tasks': 0, 'failed_tasks_ratio': 0.0}, inplace=True)

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
        full_tbl.fillna({'cache_hit_ratio': 0.0}, inplace=True)
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

    # add data source features
    ds_tbl = combine_tables('ds_tbl')
    grouped_ds_tbl = ds_tbl.groupby(['appId', 'sqlID'], as_index=False).sum()
    grouped_ds_tbl['scan_bw'] = (
        1.0 * grouped_ds_tbl['data_size'] / grouped_ds_tbl['scan_time']
    )
    ds_cols = [
        'appId',
        'sqlID',
        'scan_bw',
        'scan_time',
        'decode_time',
        'data_size',
    ]
    full_tbl = full_tbl.merge(
        grouped_ds_tbl[ds_cols], on=['appId', 'sqlID'], how='left'
    )

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

    # normalize byte features
    byte_features = [
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

    for cc in byte_features:
        full_tbl[cc + 'Ratio'] = full_tbl[cc + '_sum'] / full_tbl['input_bytesRead_sum']
        full_tbl[cc + 'Ratio'] = (
            full_tbl[cc + 'Ratio'].replace([np.inf, -np.inf], 0).fillna(0)
        )

    for cc in byte_features:
        full_tbl[cc + '_mean'] = full_tbl[cc + '_sum'] / full_tbl['numTasks_sum']
        full_tbl[cc + '_mean'] = (
            full_tbl[cc + '_mean'].replace([np.inf, -np.inf], 0).fillna(0)
        )

    # normalize time features
    time_features = [
        'executorCPUTime',
        'executorDeserializeCPUTime',
        'executorDeserializeTime',
        'executorRunTime',
        'jvmGCTime',
        'sr_fetchWaitTime',
        'sw_writeTime',
    ]

    for cc in time_features:
        full_tbl[cc + '_mean'] = full_tbl[cc + '_sum'] / full_tbl['numTasks_sum']
        full_tbl[cc + '_mean'] = (
            full_tbl[cc + '_mean'].replace([np.inf, -np.inf], 0).fillna(0)
        )

    # remove sum features
    full_tbl.drop(columns=[cc + '_sum' for cc in byte_features], inplace=True)
    full_tbl.drop(columns=[cc + '_sum' for cc in time_features], inplace=True)

    # impute inf/nan
    full_tbl[ds_cols] = full_tbl[ds_cols].replace([np.inf, -np.inf], 0).fillna(0)

    # warn if any appIds are missing after preprocessing
    missing_app_ids = list(set(unique_app_ids) - set(full_tbl['appId'].unique()))
    if missing_app_ids:
        log_fallback(logger, missing_app_ids, fallback_reason='Missing features after preprocessing')
    return full_tbl


class ScanTblError(Exception):
    pass


def load_csv_files(
    toc: pd.DataFrame,
    app_id: str,
    node_level_supp: Optional[pd.DataFrame],
    qualtool_filter: Optional[str],
    qualtool_output: Optional[pd.DataFrame],
    remove_failed_sql: bool = True,
) -> Dict[str, pd.DataFrame]:
    """Load profiler CSV files into memory.

    Parameters
    ----------
    toc: pd.DataFrame
        Table of contents of CSV files for the dataset.
    app_id: str
        Application ID.
    node_level_supp: pd.DataFrame
        Node-level support information used to filter out metrics associated with unsupported operators.
    qualtool_filter: str
        Type of filter to apply to the qualification tool output, either 'stage' or None.
    qualtool_output: pd.DataFrame
        Qualification tool output.
    remove_failed_sql: bool
        Remove sqlIDs with high failure rates, default: True.

    Returns
    -------
    Dict[str, pd.DataFrame]
        Dictionary of dataframes keyed by table name.
    """
    def scan_tbl(
        tb_name: str, abort_on_error: bool = False, warn_on_error: bool = True
    ) -> pd.DataFrame:
        try:
            scan_result = pd.read_csv(
                sgl_app[sgl_app['table_name'] == tb_name]['filepath'].iloc[0],
                encoding_errors='replace',
            )
        except Exception as ex:  # pylint: disable=broad-except
            if warn_on_error or abort_on_error:
                logger.debug('Failed to load %s for %s.', tb_name, app_id)
            if abort_on_error:
                raise ScanTblError() from ex
            scan_result = pd.DataFrame()
        return scan_result

    # Merge summary tables within each appId:
    sgl_app = toc[toc['appId'] == app_id]

    qual_tool_app_duration = pd.DataFrame()
    # CSV metrics from the profiling tool does not have app duration for incomplete applications.
    # Qualification tool provides an estimated app duration for these. We should replace the app
    # duration from CSV metrics with the estimated app duration from the qualification tool output.
    if qualtool_output is not None:
        qual_tool_app_duration = qualtool_output.loc[qualtool_output['App ID'] == app_id, 'App Duration']

    # Load summary tables:
    app_info = scan_tbl('application_information')
    if not app_info.empty and not qual_tool_app_duration.empty:
        # TODO: Update 'durationStr' if it is included as a model feature in the future.
        app_info['duration'] = qual_tool_app_duration.iloc[0]

    # Allow user-provided 'ds_name' as 'appName'
    # appName = app_info['appName'].iloc[0]
    app_name = sgl_app['ds_name'].iloc[0]

    if not app_info.empty:
        app_info['appName'] = app_name
        app_info.fillna({'sparkVersion': 'Unknown'}, inplace=True)

    # Get jar versions:
    cudf_version = '-'
    rapids_4_spark_version = '-'
    bm_runner_version = '-'

    jars_tbl = scan_tbl('rapids_accelerator_jar_and_cudf_jar', warn_on_error=False)
    if not jars_tbl.empty:
        jars_list = list(jars_tbl['Rapids4Spark jars'].str.split('/').str[-1].str[:-4])

        # Parse cudfVersion, rapids4sparkVersion, bmRunnerVersion:
        for jar in jars_list:
            if jar.startswith('cudf'):
                cudf_version = jar

            if jar.startswith('rapids-4-spark_'):
                rapids_4_spark_version = jar

            if jar.startswith('rapids-4-spark-benchmarks_'):
                bm_runner_version = jar

    if not app_info.empty:
        app_info['cudfVersion'] = cudf_version
        app_info['rapids4sparkVersion'] = rapids_4_spark_version
        app_info['bmRunnerVersion'] = bm_runner_version

    spark_props = scan_tbl('spark_properties')
    if not spark_props.empty:
        spark_props = spark_props.set_index('propertyName')

    exec_info = scan_tbl('executor_information')

    sql_duration = scan_tbl('sql_duration_and_executor_cpu_time_percent')
    if not sql_duration.empty:
        # Replace app duration with the app duration from the qualification tool output. See details above.
        if not qual_tool_app_duration.empty:
            sql_duration['App Duration'] = qual_tool_app_duration.iloc[0]
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

    # filter out sql ids that have no execs associated with them
    # this should remove root sql ids in 3.4.1+
    sql_to_stage = scan_tbl('sql_to_stage_information')
    if not sql_to_stage.empty:
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
    stages_supp = pd.DataFrame(columns=['appId', 'sqlID', 'stageIds'])
    if not sql_ops_metrics.empty and not app_info.empty:
        sql_ops_metrics['appId'] = app_info['appId'].iloc[0].strip()
        sql_ops_metrics['appName'] = app_name
        if node_level_supp is not None:
            if qualtool_filter == 'stage':
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
                # compute supported stages for use below. TBD.
                # rename column from 'Exec Is Supported' to 'Stage Is Supported' and change elsewhere
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
            else:  # qual_tool_filter_by = 'sqlId'
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

    # sql ids to drop due to failures meeting below criteria
    sqls_to_drop = set()

    # Load job+stage level agg metrics:
    job_agg_tbl = scan_tbl('job_level_aggregated_task_metrics')
    stage_agg_tbl = scan_tbl('stage_level_aggregated_task_metrics')
    job_stage_agg_tbl = pd.DataFrame()
    if not any([job_agg_tbl.empty, stage_agg_tbl.empty, job_map_tbl.empty]):
        # Rename jobId and stageId to ID
        job_df = job_agg_tbl.rename(columns={'jobId': 'ID'})
        job_df['ID'] = 'job_' + job_df['ID'].astype(str)
        stage_df = stage_agg_tbl.rename(columns={'stageId': 'ID'})
        stage_df['ID'] = 'stage_' + stage_df['ID'].astype(str)

        # Concatenate the DataFrames.
        # TODO: This is a temporary solution to minimize changes in existing code.
        #        We should refactor this once we have updated the code with latest changes.
        job_stage_agg_tbl = pd.concat([job_df, stage_df], ignore_index=True)
        job_stage_agg_tbl = job_stage_agg_tbl.rename(
            columns={'numTasks': 'numTasks_sum', 'duration_avg': 'duration_mean'}
        )
        job_stage_agg_tbl['appId'] = app_info['appId'].iloc[0]
        job_stage_agg_tbl['appName'] = app_name

        # Only need job level aggs for now since one-to-many relationships between stageID and sqlID.
        job_stage_agg_tbl['js_type'] = job_stage_agg_tbl['ID'].str.split('_').str[0]

        # mark for removal from modeling sql ids that have failed stage time > 10% total stage time
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
                stage_agg_tbl[['sqlID', 'Duration']]
                .groupby('sqlID')
                .agg('sum')
                .reset_index()
            )
            failed_stage_time = (
                stage_agg_tbl[['sqlID', 'ID', 'Duration']]
                .merge(failed_stages, left_on='ID', right_on='stageId', how='inner')[
                    ['sqlID', 'Duration']
                ]
                .groupby('sqlID')
                .agg('sum')
                .reset_index()
            )
            stage_times = total_stage_time.merge(
                failed_stage_time, on='sqlID', how='inner'
            )
            sqls_to_drop = set(
                stage_times.loc[
                    stage_times.Duration_y
                    > stage_times.Duration_x * allowed_failed_duration_fraction
                ]['sqlID']
            )

        if remove_failed_sql and sqls_to_drop:
            logger.debug('Ignoring sqlIDs %s due to excessive failed/cancelled stage duration.', sqls_to_drop)

        if node_level_supp is not None and (qualtool_filter == 'stage'):
            job_stage_agg_tbl = job_stage_agg_tbl[
                job_stage_agg_tbl['js_type'] == 'stage'
            ]
            job_stage_agg_tbl['ID'] = (
                job_stage_agg_tbl['ID']
                .apply(lambda row_id: int(row_id.split('_')[1]))
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

    # Load whole stage operator info:

    whole_stage_tbl = scan_tbl('wholestagecodegen_mapping', warn_on_error=False)
    if not whole_stage_tbl.empty:
        whole_stage_tbl['appId'] = app_info['appId'].iloc[0]

    # Merge certain tables:
    if not any(
        [app_info.empty, exec_info.empty, sql_app_metrics.empty, sql_duration.empty]
    ):
        if len(exec_info) > 1:
            # The assumption that exec_info has only 1 row. Put a warning message to capture that case.
            logger.warning('executor_information csv file has multiple rows. AppID [%s]', app_id)
        app_info_mg = app_info.merge(exec_info, how='cross')
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
        app_info_mg = app_info_mg.drop(columns=['appID', 'App ID'])

        # filter out sqlIDs with aborted jobs (these are jobs failed due to sufficiently many (configurable) failed
        # attempts of a stage due to error conditions). these are failed sqlIDs that we shouldn't model,
        # but are still included in profiler output.
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
            logger.debug('Ignoring sqlIDs %s due to aborted jobs.', aborted_sql_ids)

        sqls_to_drop = sqls_to_drop.union(aborted_sql_ids)

        if remove_failed_sql and sqls_to_drop:
            logger.debug(
                'Ignoring failed sqlIDs due to stage/job failures for %s: %s',
                app_id,
                ', '.join(map(str, sqls_to_drop))
            )
            app_info_mg = app_info_mg.loc[~app_info_mg.sqlID.isin(sqls_to_drop)]

    else:
        app_info_mg = pd.DataFrame()

    # Read failed_tasks table
    failed_tasks = scan_tbl('failed_tasks', warn_on_error=False)
    if not failed_tasks.empty:
        # add appId and appName
        failed_tasks['appId'] = app_info['appId'].iloc[0]
        failed_tasks['appName'] = app_name
        # aggregate failed tasks per appName, appId, sqlID
        failed_tasks = failed_tasks.groupby(['appName', 'appId', 'stageId'])['attempt'].count().reset_index()
        failed_tasks = failed_tasks.merge(sql_to_stage[['stageId', 'sqlID']], on=['stageId'])
        failed_tasks = failed_tasks.groupby(['appName', 'appId', 'sqlID'])['attempt'].sum().reset_index()
        failed_tasks = failed_tasks.rename(columns={'attempt': 'failed_tasks'})
    else:
        failed_tasks = pd.DataFrame(
            columns=['appName', 'appId', 'sqlID', 'failed_tasks']
        ).astype({'sqlID': int, 'failed_tasks': int})

    # Read data_source_information table
    ds_tbl = scan_tbl('data_source_information')
    if not ds_tbl.empty:
        ds_tbl['appId'] = app_info['appId'].iloc[0]

    out = {
        'app_tbl': app_info_mg,
        'ops_tbl': sql_ops_metrics,
        'spark_props_tbl': spark_props,
        'job_map_tbl': job_map_tbl,
        'job_stage_agg_tbl': job_stage_agg_tbl,
        'wholestage_tbl': whole_stage_tbl,
        'ds_tbl': ds_tbl,
        'failed_tasks_tbl': failed_tasks,
    }

    return out
