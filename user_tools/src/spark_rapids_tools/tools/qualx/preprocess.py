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

from itertools import chain
from pathlib import Path
from typing import Any, List, Mapping, Optional, Tuple
import json
import glob
import numpy as np
import os
import pandas as pd
from spark_rapids_tools.tools.qualx.util import (
    ensure_directory,
    find_eventlogs,
    find_paths,
    get_cache_dir,
    get_logger,
    get_dataset_platforms,
    run_profiler_tool, log_fallback,
)

PREPROCESSED_FILE = 'preprocessed.parquet'

logger = get_logger(__name__)

# determing if a containing stage or sqlID is fully supported
unsupported_overrides = [
    'AdaptiveSparkPlan',
    'AppendDataExecV1',
    'AtomicReplaceTableAsSelect',
    'BroadcastNestedLoopJoin',
    'ColumnarToRow',
    'CommandResult',
    'DeltaInvariantChecker',
    'Execute AddJarsCommand',
    'Execute MergeIntoCommandEdge',
    'OverwriteByExpressionExecV1',
    'ResultQueryStage',
    'Scan ExistingRDD',
    'Scan ExistingRDD Delta Table',
    'Scan ExistingRDD mergeMaterializedSource',
    'SortMergeJoin(skew=true)',
    'Subquery',
    'TableCacheQueryStage',
]
# expected features for dataframe produced by preprocessing
expected_raw_features = \
    {
        'appDuration',
        'appId',
        'appName',
        'cache_hit_ratio',
        'data_size',
        'decode_time',
        'description',
        'diskBytesSpilled_mean',
        'diskBytesSpilledRatio',
        'duration_max',
        'duration_mean',
        'duration_min',
        'duration_sum',
        'Duration',
        'executorCores',
        'executorCPUTime_mean',
        'executorDeserializeCPUTime_mean',
        'executorDeserializeTime_mean',
        'executorMemory',
        'executorOffHeap',
        'executorRunTime_mean',
        'fraction_supported',
        'input_bytesRead_mean',
        'input_bytesReadRatio',
        'input_recordsRead_sum',
        'jvmGCTime_mean',
        'maxMem',
        'maxOffHeapMem',
        'maxOnHeapMem',
        'memoryBytesSpilled_mean',
        'memoryBytesSpilledRatio',
        'numExecutors',
        'numGpusPerExecutor',
        'numTasks_sum',
        'output_bytesWritten_mean',
        'output_bytesWrittenRatio',
        'output_recordsWritten_sum',
        'peakExecutionMemory_max',
        'platform_databricks-aws',
        'platform_databricks-azure',
        'platform_dataproc',
        'platform_emr',
        'platform_onprem',
        'pluginEnabled',
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
        'sqlOp_BatchEvalPython',
        'sqlOp_BroadcastExchange',
        'sqlOp_BroadcastHashJoin',
        'sqlOp_BroadcastNestedLoopJoin',
        'sqlOp_CartesianProduct',
        'sqlOp_ColumnarToRow',
        'sqlOp_CommandResult',
        'sqlOp_CustomShuffleReader',
        'sqlOp_DeserializeToObject',
        'sqlOp_Exchange',
        'sqlOp_Execute InsertIntoHadoopFsRelationCommand',
        'sqlOp_Expand',
        'sqlOp_Filter',
        'sqlOp_Generate',
        'sqlOp_GenerateBloomFilter',
        'sqlOp_GlobalLimit',
        'sqlOp_HashAggregate',
        'sqlOp_HashAggregatePrefixGroupingSets',
        'sqlOp_LocalLimit',
        'sqlOp_LocalTableScan',
        'sqlOp_MapElements',
        'sqlOp_ObjectHashAggregate',
        'sqlOp_OutputAdapter',
        'sqlOp_PartialWindow',
        'sqlOp_Project',
        'sqlOp_ReusedSort',
        'sqlOp_RunningWindowFunction',
        'sqlOp_Scan csv ',
        'sqlOp_Scan ExistingRDD Delta Table Checkpoint',
        'sqlOp_Scan ExistingRDD Delta Table State',
        'sqlOp_Scan JDBCRelation',
        'sqlOp_Scan json ',
        'sqlOp_Scan OneRowRelation',
        'sqlOp_Scan orc ',
        'sqlOp_Scan parquet ',
        'sqlOp_Scan text ',
        'sqlOp_SerializeFromObject',
        'sqlOp_Sort',
        'sqlOp_SortAggregate',
        'sqlOp_SortMergeJoin',
        'sqlOp_Subquery',
        'sqlOp_SubqueryBroadcast',
        'sqlOp_SubqueryOutputBroadcast',
        'sqlOp_TakeOrderedAndProject',
        'sqlOp_Window',
        'sqlOp_WindowGroupLimit',
        'sqlOp_WindowSort',
        'sr_fetchWaitTime_mean',
        'sr_localBlocksFetched_sum',
        'sr_localBytesRead_mean',
        'sr_localBytesReadRatio',
        'sr_remoteBlocksFetched_sum',
        'sr_remoteBytesRead_mean',
        'sr_remoteBytesReadRatio',
        'sr_remoteBytesReadToDisk_mean',
        'sr_remoteBytesReadToDiskRatio',
        'sr_totalBytesRead_mean',
        'sr_totalBytesReadRatio',
        'sw_bytesWritten_mean',
        'sw_bytesWrittenRatio',
        'sw_recordsWritten_sum',
        'sw_writeTime_mean',
        'taskCpu',
        'taskGpu',
    }


def load_datasets(
    dataset: str, ignore_test=True
) -> Tuple[Mapping[str, Any], pd.DataFrame]:
    """Load datasets as JSON and return a pd.DataFrame from the profiler CSV files.

    Parameters
    ----------
    dataset:
        Path to a folder containing one or more dataset JSON files.
    """
    platforms, dataset_base = get_dataset_platforms(dataset)

    cache_dir = get_cache_dir()
    if ignore_test:
        filter_fn = lambda f: f.endswith('.json') and 'test' not in f
    else:
        filter_fn = lambda f: f.endswith('.json')

    ds_count = 0
    all_datasets = {}
    profile_dfs = []
    for platform in platforms:
        # load json files
        datasets = {}
        json_files = find_paths(f'{dataset_base}/{platform}', filter_fn)
        for json_file in json_files:
            with open(json_file, 'r') as f:
                ds = json.load(f)
                ds_count += len(ds)
                for ds_name in ds.keys():
                    # inject platform into dataset metadata
                    ds[ds_name]['platform'] = platform
                datasets.update(ds)
        all_datasets.update(datasets)

        # check cache
        platform_cache = f'{cache_dir}/{platform}'
        ensure_directory(platform_cache)
        if os.path.isfile(f'{platform_cache}/{PREPROCESSED_FILE}'):
            # load preprocessed input, if cached
            logger.info(
                f'Using cached profile_df: {platform_cache}/{PREPROCESSED_FILE}'
            )
            profile_df = pd.read_parquet(f'{platform_cache}/{PREPROCESSED_FILE}')
            if ignore_test:
                # remove any 'test' datasets from preprocessed data
                dataset_keys = list(datasets.keys())
                profile_df = profile_df.loc[profile_df['appName'].isin(dataset_keys)]
        else:
            # otherwise, check for cached profiler output
            profile_dir = f'{platform_cache}/profile'
            ensure_directory(profile_dir)
            profiles = os.listdir(profile_dir)

            # run the profiler on eventlogs, if not already cached
            for ds_name, ds_meta in datasets.items():
                if ds_name not in profiles:
                    eventlogs = ds_meta['eventlogs']
                    for eventlog in eventlogs:
                        eventlog = os.path.expandvars(eventlog)
                        run_profiler_tool(
                            platform, eventlog, f'{profile_dir}/{ds_name}'
                        )
            # load/preprocess profiler data
            profile_df = load_profiles(datasets, profile_dir)
            # save preprocessed dataframe to cache
            profile_df.to_parquet(f'{platform_cache}/{PREPROCESSED_FILE}')

        profile_dfs.append(profile_df)

    profile_df = pd.concat(profile_dfs)

    # sanity check
    if ds_count != len(all_datasets):
        logger.warn(
            f'Duplicate dataset key detected, got {len(all_datasets)} datasets, but read {ds_count} datasets.'
        )

    return all_datasets, profile_df


def load_profiles(
    datasets: Mapping[str, Mapping],
    profile_dir: Optional[str] = None,
    node_level_supp: Optional[pd.DataFrame] = None,
    qualtool_filter: Optional[str] = None,
    qualtool_output: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """Load dataset profiler CSV files as a pd.DataFrame."""

    def infer_app_meta(eventlogs: List[str]) -> Mapping[str, Mapping]:
        eventlog_list = [find_eventlogs(os.path.expandvars(e)) for e in eventlogs]
        eventlog_list = list(chain(*eventlog_list))
        app_meta = {}
        for e in eventlog_list:
            parts = Path(e).parts
            appId = parts[-1]
            runType = parts[-2].upper()
            description = parts[-4]
            app_meta[appId] = {
                'runType': runType,
                'description': description,
                'scaleFactor': 1,
            }
        return app_meta

    all_raw_features = []
    # get list of csv files from each profile
    for ds_name, ds_meta in datasets.items():
        toc_list = []
        app_meta = ds_meta.get('app_meta', None)
        platform = ds_meta.get('platform', 'onprem')
        scalefactor_meta = ds_meta.get('scaleFactorFromSqlIDRank', None)

        if not app_meta:
            # if no 'app_meta' key provided, infer app_meta from directory structure of eventlogs
            app_meta = infer_app_meta(ds_meta['eventlogs'])

        # TODO: There is a difference in the way qualification tool and qualx train/predict consume profiling output.
        # We should clean this up in the future.
        if 'profiles' in ds_meta:
            profile_paths = ds_meta['profiles']
        elif profile_dir is None:
            logger.error('No profiles provided or profile_dir specified.')
            continue
        elif 'query_per_app' in ds_name:
            # don't return list of profile paths, since we'll glob by appId pattern later
            profile_paths = [f'{profile_dir}/{ds_name}']
        else:
            profile_paths = glob.glob(f'{profile_dir}/{ds_name}/*')
        for path in profile_paths:
            for appId in app_meta.keys():
                if appId == 'default':
                    csv_files = glob.glob(f'{path}/**/*.csv', recursive=True)
                else:
                    csv_files = glob.glob(f'{path}/**/{appId}/*.csv', recursive=True)
                if csv_files:
                    tmp = pd.DataFrame({'filepath': csv_files})
                    fp_split = tmp['filepath'].str.split(r'/')
                    tmp['test_name'] = ds_name
                    tmp['appId'] = fp_split.str[-2]
                    tmp['table_name'] = fp_split.str[-1].str[:-4]
                    tmp['runType'] = (
                        app_meta[appId]['runType']
                        if appId in app_meta
                        else app_meta['default']['runType']
                    )
                    if not scalefactor_meta:
                        tmp['scaleFactor'] = (
                            app_meta[appId]['scaleFactor']
                            if appId in app_meta
                            else app_meta['default']['scaleFactor']
                        )
                    toc_list.append(tmp)

        if not toc_list:
            raise ValueError(f'No CSV files found for: {ds_name}')
        else:
            toc = pd.concat(toc_list)
            raw_features = extract_raw_features(toc, node_level_supp, qualtool_filter, qualtool_output)
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

            # override description from app_meta (if available)
            if 'description' in app_meta[list(app_meta.keys())[0]]:
                app_desc = {
                    appId: meta['description']
                    for appId, meta in app_meta.items()
                    if 'description' in meta
                }
                raw_features['description'] = raw_features['appId'].map(app_desc)
                # append also to appName to allow joining cpu and gpu logs at the app level
                raw_features['appName'] = (
                    raw_features['appName'] + '_' + raw_features['description']
                )

            # add platform from app_meta
            raw_features[f'platform_{platform}'] = 1
            raw_features = impute(raw_features)
            all_raw_features.append(raw_features)
    return (
        pd.concat(all_raw_features).reset_index(drop=True)
        if all_raw_features
        else pd.DataFrame()
    )


def extract_raw_features(
    toc: pd.DataFrame,
    node_level_supp: Optional[pd.DataFrame],
    qualtool_filter: Optional[str],
    qualtool_output: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """Given a pandas dataframe of CSV files, extract raw features into a single dataframe keyed by (appId, sqlID)."""
    # read all tables per appId
    unique_app_ids = toc['appId'].unique()
    appId_tables = [
        load_csv_files(toc, appId, node_level_supp, qualtool_filter, qualtool_output)
        for appId in unique_app_ids
    ]

    def combine_tables(table_name: str) -> pd.DataFrame:
        """Combine csv tables (by name) across all appIds."""
        merged = pd.concat(
            [appId_tables[i][table_name] for i in range(len(appId_tables))]
        )
        return merged

    app_tbl = combine_tables('app_tbl').sort_values('startTime', ascending=True)
    ops_tbl = combine_tables('ops_tbl')

    # job_map_tbl = combine_tables('job_map_tbl')
    job_stage_agg_tbl = combine_tables('job_stage_agg_tbl')
    wholestage_tbl = combine_tables('wholestage_tbl')
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
        return pd.DataFrame()

    # normalize dtypes
    app_int_dtypes = ['taskCpu', 'taskGpu']
    app_tbl[app_int_dtypes] = app_tbl[app_int_dtypes].fillna(0).astype(int)

    # normalize timings from ns to ms
    nsTiming_mask = ops_tbl['metricType'] == 'nsTiming'
    ops_tbl.loc[nsTiming_mask, 'max'] = (
        ops_tbl.loc[nsTiming_mask, 'max'] / 1e6
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
    except Exception:
        wholestage_tbl_filter = pd.DataFrame()

    # remove WholeStageCodegen from original ops table and replace with constituent ops
    ops_tbl_filter = ops_tbl[ops_tbl['nodeName'] != 'WholeStageCodegen']
    ops_tbl_filter = (
        ops_tbl_filter.groupby(['appId', 'sqlID', 'nodeID'])['nodeName']
        .first()
        .reset_index()
    )
    sql_ops_counter = pd.concat([ops_tbl_filter, wholestage_tbl_filter])

    # normalize sqlOp labels w/ variable suffixes
    # note: have to do this after unpacking WholeStageCodegen ops
    dynamic_op_labels = [
        # CPU
        'Scan DeltaCDFRelation',
        'Scan ExistingRDD Delta Table Checkpoint',
        'Scan ExistingRDD Delta Table State',
        'Scan JDBCRelation',
        'Scan parquet ',  # trailing space is also in default sql op name
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
    sql_job_agg_tbl['hasSqlID'] = sql_job_agg_tbl['sqlID'] >= 0
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


def impute(full_tbl: pd.DataFrame) -> pd.DataFrame:
    """Impute missing columns and delete extra columns."""
    actual_features = set(full_tbl.columns)
    if actual_features == expected_raw_features:
        logger.info('Dataset has all expected features')
    else:
        missing = sorted(expected_raw_features - actual_features)
        extra = sorted(actual_features - expected_raw_features)
        if missing:
            logger.warn(f'Imputing missing features: {missing}')
            for col in missing:
                if col != 'fraction_supported':
                    full_tbl[col] = 0
                else:
                    full_tbl[col] = 1.0

        if extra:
            logger.warn(f'Removing extra features: {extra}')
            full_tbl = full_tbl.drop(columns=extra)

        # one last check after modifications (update expected_raw_features if needed)
        assert set(full_tbl.columns) == expected_raw_features

    return full_tbl


class ScanTblError(Exception):
    pass


def load_csv_files(
    toc: pd.DataFrame,
    app_id: str,
    node_level_supp: Optional[pd.DataFrame],
    qualtool_filter: Optional[str],
    qualtool_output: Optional[pd.DataFrame],
) -> List[Mapping[str, pd.DataFrame]]:
    """
    Load profiler CSV files into memory.
    """

    def scan_tbl(
        tb_name: str, abort_on_error: bool = False, warn_on_error: bool = True
    ) -> pd.DataFrame:
        try:
            out = pd.read_csv(
                sgl_app[sgl_app['table_name'] == tb_name]['filepath'].iloc[0],
                encoding_errors='replace',
            )
        except Exception:
            if warn_on_error or abort_on_error:
                logger.warn(f'Failed to load {tb_name} for {app_id}.')
            if abort_on_error:
                raise ScanTblError()
            out = pd.DataFrame()
        return out

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

    # Allow user-provided 'test_name' as 'appName'
    # appName = app_info['appName'].iloc[0]
    appName = sgl_app['test_name'].iloc[0]

    if not app_info.empty:
        app_info['appName'] = appName
        app_info['sparkVersion'].fillna('Unknown', inplace=True)

    # Get jar versions:
    cudfVersion = '-'
    rapids4sparkVersion = '-'
    bmRunnerVersion = '-'

    jars_tbl = scan_tbl('rapids_accelerator_jar_and_cudf_jar', warn_on_error=False)
    if not jars_tbl.empty:
        jars_list = list(jars_tbl['Rapids4Spark jars'].str.split('/').str[-1].str[:-4])

        # Parse cudfVersion, rapids4sparkVersion, bmRunnerVersion:
        for jar in jars_list:
            if jar.startswith('cudf'):
                cudfVersion = jar

            if jar.startswith('rapids-4-spark_'):
                rapids4sparkVersion = jar

            if jar.startswith('rapids-4-spark-benchmarks_'):
                bmRunnerVersion = jar

    if not app_info.empty:
        app_info['cudfVersion'] = cudfVersion
        app_info['rapids4sparkVersion'] = rapids4sparkVersion
        app_info['bmRunnerVersion'] = bmRunnerVersion

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
    if not sql_app_metrics.empty:
        sql_app_metrics = sql_app_metrics.drop(columns='appIndex')

    # filter out sql ids that have no execs associated with them
    # this should remove root sql ids in 3.4.1+
    sql_to_stage = scan_tbl('sql_to_stage_information')
    if not sql_to_stage.empty:
        sql_to_stage['SQL Nodes(IDs)']
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
        sql_ops_metrics['appName'] = appName
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
                # compute supported stages for use below. TBD.  rename column from 'Exec Is Supported' to 'Stage Is Supported' and change elsewhere
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
        job_stage_agg_tbl = job_stage_agg_tbl.drop(columns="appIndex")
        job_stage_agg_tbl = job_stage_agg_tbl.rename(
            columns={'numTasks': 'numTasks_sum', 'duration_avg': 'duration_mean'}
        )
        job_stage_agg_tbl['appId'] = app_info['appId'].iloc[0]
        job_stage_agg_tbl['appName'] = appName

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
            stage_times.info()
            sqls_to_drop = set(
                stage_times.loc[
                    stage_times.Duration_y
                    > stage_times.Duration_x * allowed_failed_duration_fraction
                ]['sqlID']
            )

        if sqls_to_drop:
            logger.warn(
                f'Ignoring sqlIDs {sqls_to_drop} due to excessive failed/cancelled stage duration.'
            )

        if node_level_supp is not None and (qualtool_filter == 'stage'):
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
            logger.warn(f'Ignoring sqlIDs {aborted_sql_ids} due to aborted jobs.')

        sqls_to_drop = sqls_to_drop.union(aborted_sql_ids)

        if sqls_to_drop:
            logger.warn(
                f'Ignoring a total of {len(sqls_to_drop)} sqlIDs due to stage/job failures.'
            )
            app_info_mg = app_info_mg.loc[~app_info_mg.sqlID.isin(sqls_to_drop)]

    else:
        app_info_mg = pd.DataFrame()

    ds_tbl = scan_tbl('data_source_information')
    if not ds_tbl.empty:
        ds_tbl['appId'] = app_info['appId'].iloc[0]

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


def load_qtool_execs(qtool_execs: List[str]) -> Optional[pd.DataFrame]:
    """
    Load supported stage info from qtool output in a form that can be merged with profiler data
    to aggregate features and durations only over supported stages.
    """
    node_level_supp = None
    if qtool_execs:
        exec_info = pd.concat([pd.read_csv(f) for f in qtool_execs])
        node_level_supp = exec_info.copy()
        node_level_supp['Exec Is Supported'] = (
            node_level_supp['Exec Is Supported']
            | node_level_supp['Exec Name'].apply(
                lambda x: any([x.startswith(nm) for nm in unsupported_overrides])
            )
            | node_level_supp['Exec Name'].apply(
                lambda x: x.startswith('WholeStageCodegen')
            )
        )
        node_level_supp = (
            node_level_supp[['App ID', 'SQL ID', 'SQL Node Id', 'Exec Is Supported']]
            .groupby(['App ID', 'SQL ID', 'SQL Node Id'])
            .agg('all')
            .reset_index(level=[0, 1, 2])
        )
    return node_level_supp


def load_qual_csv(
    qual_dirs: List[str], csv_filename: str, cols: Optional[List[str]] = None
) -> Optional[pd.DataFrame]:
    """
    Load CSV file from qual tool output as pandas DataFrame.
    """
    qual_csv = [os.path.join(q, csv_filename) for q in qual_dirs]
    df = None
    if qual_csv:
        df = pd.concat([pd.read_csv(f) for f in qual_csv])
        if cols:
            df = df[cols]
    return df
