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

"""Utility functions for featurizer for QualX"""

from typing import List, Dict, Optional

import pandas as pd

from spark_rapids_tools.api_v1 import LoadRawFilesResult, ToolResultHandlerT, CSVReportCombiner, CSVReport, \
    APIUtils
from spark_rapids_tools.tools.qualx.preprocess import load_qtool_execs

from spark_rapids_tools.utils import Utilities


def load_csv_files(
        ds_name: str,
        res_hs: List[ToolResultHandlerT]
) -> LoadRawFilesResult:
    """
    Load CSV files from the result handlers into memory. This method should minimize applying logic
    on the dataframes as mush as possible in order to give a clear separation between loading
    raw-data Vs. processing them to create features.
    The implementation uses CSVReportCombiner to combine the reports across all the apps.
    By default, the combiner injects the appId column if it does not exist in the report.
    :param ds_name: The name of the dataset.
    :param res_hs: List of tools result handlers. The assumptions that each handler is valid and has
           non-zero apps.
    :return: A LoadRawFilesResult result object.
    :raises: RuntimeError: If unexpected error is triggered while loading a mandatory report.
    """
    raw_res = LoadRawFilesResult(ds_name=ds_name)

    # Get the app_info report.
    # Raise when empty/failed to load.
    # The API will not inject appId into the result because the report has the appId column already.
    APIUtils.process_res(
        raw_res=raw_res,
        combiner=CSVReportCombiner(
            rep_builders=[CSVReport(r_h).table('coreRawApplicationInformationCSV') for r_h in res_hs]
        )
    )

    # Get the jars table.
    # Do not fail if it is empty/failed. This is empty for CPU applications.
    APIUtils.process_res(
        raw_res=raw_res,
        combiner=CSVReportCombiner(
            rep_builders=[CSVReport(r_h).table('coreRawRapidsJarsCSV') for r_h in res_hs]
        ),
        raise_on_empty=False,
        raise_on_failure=False,
    )

    # Get the spark properties table (this should not be empty)
    # Raise when empty/failed to load.
    # TODO: It does not seem that this table is actually used anywhere
    APIUtils.process_res(
        raw_res=raw_res,
        combiner=CSVReportCombiner(
            rep_builders=[CSVReport(r_h).table('coreRawSparkPropertiesCSV') for r_h in res_hs]
        )
    )

    # Get the executor time-percent table (this should not be empty)
    # Raise if empty or failed to load.
    # This table already has "App ID" column for some reason. Rename the column to appId and disable
    # app injection.
    APIUtils.process_res(
        raw_res=raw_res,
        combiner=CSVReportCombiner(
            rep_builders=[
                CSVReport(r_h)
                .map_cols_cb(APIUtils.normalize_app_id_col)  # normalize the "App ID" column to 'appId'
                .table('coreRawSqlDurationAndExecutorCpuTimePercentCSV') for r_h in res_hs
            ]
        ).disable_apps_injection()  # disable app-injection
    )

    # Get the executor information table
    APIUtils.process_res(
        raw_res=raw_res,
        combiner=CSVReportCombiner(
            rep_builders=[CSVReport(r_h).table('coreRawExecutorInformationCSV') for r_h in res_hs]
        )
    )

    # Get the sql level aggregated metrics (this might be empty if no SQLs/metrics are defined)
    # Raise if empty or failed to load
    # This table already has  "appID". Rename the column to appId and avoid injecting the appId column.
    APIUtils.process_res(
        raw_res=raw_res,
        combiner=CSVReportCombiner(
            rep_builders=[
                CSVReport(r_h)
                .map_cols_cb(APIUtils.normalize_app_id_col)
                .table('coreRawSqlLevelAggregatedTaskMetricsCSV') for r_h in res_hs
            ]
        ).disable_apps_injection()
    )

    # Get sql-to-stage info (this can be empty if stages are not defined or sqls are not defined)
    APIUtils.process_res(
        raw_res=raw_res,
        combiner=CSVReportCombiner(
            rep_builders=[CSVReport(r_h).table('coreRawSqlToStageInformationCSV') for r_h in res_hs]
        ),
        raise_on_empty=False
    )

    # Get job info
    APIUtils.process_res(
        raw_res=raw_res,
        combiner=CSVReportCombiner(
            rep_builders=[CSVReport(r_h).table('coreRawJobInformationCSV') for r_h in res_hs]
        )
    )

    # Get the sql-plan-metrics. This cannot be empty.
    APIUtils.process_res(
        raw_res=raw_res,
        combiner=CSVReportCombiner(
            rep_builders=[CSVReport(r_h).table('coreRawSqlPlanMetricsForApplicationCSV') for r_h in res_hs]
        )
    )

    # Get the job-agg-metrics. This cannot be empty.
    APIUtils.process_res(
        raw_res=raw_res,
        combiner=CSVReportCombiner(
            rep_builders=[CSVReport(r_h).table('coreRawJobLevelAggregatedTaskMetricsCSV') for r_h in res_hs]
        )
    )

    # Get the stage-agg-metrics. This cannot be empty.
    APIUtils.process_res(
        raw_res=raw_res,
        combiner=CSVReportCombiner(
            rep_builders=[CSVReport(r_h).table('coreRawStageLevelAggregatedTaskMetricsCSV') for r_h in res_hs]
        )
    )

    # Get whole stage operator info. It can be empty; especially for the GPU jobs.
    APIUtils.process_res(
        raw_res=raw_res,
        combiner=CSVReportCombiner(
            rep_builders=[CSVReport(r_h).table('coreRawWholeStageCSV') for r_h in res_hs]
        ),
        raise_on_empty=False,
        raise_on_failure=False
    )

    # Get failed-tasks info. It can be empty.
    APIUtils.process_res(
        raw_res=raw_res,
        combiner=CSVReportCombiner(
            rep_builders=[CSVReport(r_h).table('coreRawFailedTasksCSV') for r_h in res_hs]
        ),
        raise_on_empty=False,
        raise_on_failure=False
    )

    # Get failed-stages info. It can be empty.
    APIUtils.process_res(
        raw_res=raw_res,
        combiner=CSVReportCombiner(
            rep_builders=[CSVReport(r_h).table('coreRawFailedStagesCSV') for r_h in res_hs]
        ),
        raise_on_empty=False,
        raise_on_failure=False
    )

    # Get job-failed info. It can be empty.
    APIUtils.process_res(
        raw_res=raw_res,
        combiner=CSVReportCombiner(
            rep_builders=[CSVReport(r_h).table('coreRawFailedJobsCSV') for r_h in res_hs]
        ),
        raise_on_empty=False,
        raise_on_failure=False
    )

    # Get data-source info. It can be empty.
    # Note that this report is error-prune due to parsing the read-operators.
    # P.S: It is wise to not fail if this report fail. this table may have lots non-deterministic
    #      behavior due to handling new read operators.
    APIUtils.process_res(
        raw_res=raw_res,
        combiner=CSVReportCombiner(
            rep_builders=[CSVReport(r_h).table('coreRawDataSourceInformationCSV') for r_h in res_hs]
        ),
        raise_on_empty=False,
        raise_on_failure=False
    )

    #########################################
    # Load Qualification reports
    #########################################

    # Gets the execs_report. We want to safely inform the reporter not to fail because
    # it should not if the result_handlers are not Qualification handlers.
    # note that there is a side effect that we do not filter the rows immediately here.
    # Instead, we combine all rows across all the apps before any filters which can be huge.

    APIUtils.process_res(
        raw_res=raw_res,
        combiner=CSVReportCombiner(
            rep_builders=[
                CSVReport(r_h)
                .table('execCSVReport')
                for r_h in res_hs
            ]
        ).on_app_fields({'app_id': 'App ID'}),  # use "App ID" to be consistent with the remaining qualx code.
        raise_on_empty=False,
        raise_on_failure=False
    )
    # TODO: there is no need to load app_summary to get the app_durations.
    #       App duration should be available in the app_info_df.

    return raw_res


def process_raw_features(
        raw_tbls: LoadRawFilesResult,
        *,
        qualtool_filter: Optional[str],
        remove_failed_sql: bool = True) -> Dict[str, pd.DataFrame]:
    """
    Process the raw features from the loaded CSV files.
    :param raw_tbls: The result of loading raw files.
    :param qualtool_filter: The type of filter to apply to the qualification tool output, either 'stage' or None.
    :param remove_failed_sql: If True, remove SQLs with high failure rates.
    :return: A DataFrame containing the processed features.
    """
    def valid_df(arg: Optional[pd.DataFrame], allow_empty: bool = False) -> bool:
        """
        Check if the DataFrame is not None and not empty.
        :param arg: The DataFrame to check.
        :param allow_empty: If True, allow the DataFrame to be empty.
        :return: True if the DataFrame is valid, False otherwise.
        """
        if arg is None:
            return False
        if arg.empty:
            # If the DataFrame is empty, we check if allow_empty is True.
            return allow_empty
        return True

    # Step-1: process the failed_apps to decide if we need to drop some of them based on the failures
    # Step-2: process the successful apps to extract the features
    #   Step-a: combine some of the columns.
    #   Step-b: return the processed DataFrame with the features.

    app_info_df = raw_tbls.reports.get('coreRawApplicationInformationCSV')

    if valid_df(app_info_df):
        # Get jar versions: This is only valid for GPU applications.
        raw_jars_df = raw_tbls.reports.get('coreRawRapidsJarsCSV')
        # Merge jar versions into app_info_df based on appId
        if valid_df(raw_jars_df):
            # Extract version info per appId
            def extract_versions(jars):
                cudf_version = '-'
                rapids4spark_version = '-'
                bm_runner_version = '-'
                for jar in jars.split(','):
                    jar = jar.strip().split('/')[-1].replace('.jar', '')
                    if jar.startswith('cudf'):
                        cudf_version = jar
                    elif jar.startswith('rapids-4-spark_'):
                        rapids4spark_version = jar
                    elif jar.startswith('rapids-4-spark-benchmarks_'):
                        bm_runner_version = jar
                return pd.Series([cudf_version, rapids4spark_version, bm_runner_version])

            jars_versions = raw_jars_df[['appId', 'Rapids4Spark jars']].copy()
            jars_versions[['cudfVersion', 'rapids4sparkVersion', 'bmRunnerVersion']] = (
                jars_versions['Rapids4Spark jars'].apply(extract_versions)
            )
            jars_versions = jars_versions[['appId', 'cudfVersion', 'rapids4sparkVersion', 'bmRunnerVersion']]

            # Merge with app_info_df
            app_info_df = app_info_df.merge(
                jars_versions,
                on='appId',
                how='left'
            )
            # Fill missing version info with '-'
            app_info_df[['cudfVersion', 'rapids4sparkVersion', 'bmRunnerVersion']] = (
                app_info_df[['cudfVersion', 'rapids4sparkVersion', 'bmRunnerVersion']].fillna('-')
            )
        else:
            # If no jars info, set all to '-'
            app_info_df['cudfVersion'] = '-'
            app_info_df['rapids4sparkVersion'] = '-'
            app_info_df['bmRunnerVersion'] = '-'

        # Allow user-provided 'ds_name' as 'appName'
        # TODO: Do we still need the line below?
        app_info_df['appName'] = raw_tbls.ds_name
        # Enforce the value of SparkVersion
        app_info_df.fillna({'sparkVersion': 'Unknown'}, inplace=True)

    ################################################
    # coreRawSqlDurationAndExecutorCpuTimePercentCSV
    ################################################
    # TODO: legacy code overwrite the SQLDuration with the AppDuration. It is not clear why we do so?
    raw_exec_dur_df = raw_tbls.reports.get('coreRawSqlDurationAndExecutorCpuTimePercentCSV')
    if valid_df(raw_exec_dur_df):
        raw_exec_dur_df = raw_exec_dur_df.rename(
            {
                'App Duration': 'appDuration',
                'Contains Dataset or RDD Op': 'containsDatasetOrRDDOp',
            },
            axis=1,
        )
        # create a column potentialProblems to indicate whether the "Potential Problems" column is equal to UDF or not.
        raw_exec_dur_df['potentialProblems'] = (
                raw_exec_dur_df['Potential Problems'] == 'UDF'
        ).fillna(False).astype(bool)
        raw_exec_dur_df.drop(columns=['Potential Problems'])

    ###############################################
    # coreRawSqlToStageInformationCSV
    # coreRawSqlLevelAggregatedTaskMetricsCSV
    ###############################################
    # filter out sql ids that have no execs associated with them
    # this should remove root sql ids in 3.4.1+
    raw_sql_to_stage_df = raw_tbls.reports.get('coreRawSqlToStageInformationCSV')
    raw_sql_agg_metrics_df = raw_tbls.reports.get('coreRawSqlLevelAggregatedTaskMetricsCSV')
    if valid_df(raw_sql_to_stage_df):
        # Filters the DataFrame raw_sql_to_stage_df to only include rows where the column SQL Nodes(IDs)
        # is not null (i.e., contains data). It then selects only the [appId, sqlID, jobID] columns from those
        # filtered rows. The result is a new DataFrame containing just the appId, sqlID and jobID
        # for entries that have associated SQL node IDs.
        sqls_with_execs = (
            raw_sql_to_stage_df.loc[raw_sql_to_stage_df['SQL Nodes(IDs)'].notna()][['appId', 'sqlID', 'jobID']]
            .groupby(['appId', 'sqlID'])  # groups the filtered rows by appId and sqlID.
            .first()                      # keeps the first occurence (i.e., 1st jobID for each unique pair).
            .reset_index()                # Resets the index to turn the groupby result back into a regular DataFrame.
        )
        # filter the sqls with execs
        if valid_df(raw_sql_agg_metrics_df) and valid_df(sqls_with_execs):
            raw_sql_agg_metrics_df = (
                raw_sql_agg_metrics_df
                .merge(sqls_with_execs, left_on=['appId', 'sqlID'], right_on=['appId', 'sqlID'])
                .drop(columns=['jobID'])
                .reset_index(drop=True)
            )

    ##################################
    # execCSVReport
    ##################################
    node_level_supp = None
    raw_execs_df = raw_tbls.reports.get('execCSVReport')
    if valid_df(raw_execs_df):
        node_level_supp = load_qtool_execs(raw_execs_df)

    ###############################################
    # coreRawSqlPlanMetricsForApplicationCSV
    ###############################################
    raw_sql_plan_metrics_df = raw_tbls.reports.get('coreRawSqlPlanMetricsForApplicationCSV')
    stages_supp = pd.DataFrame(columns=['appId', 'sqlID', 'stageIds']).astype({
            'appId': Utilities.scala_to_pandas_type('String'),
            'sqlID': Utilities.scala_to_pandas_type('Long'),
            'stageIds': Utilities.scala_to_pandas_type('Int')
        })
    if valid_df(raw_sql_plan_metrics_df):
        if node_level_supp is not None:
            if qualtool_filter == 'stage':
                # Filter out rows that do not have matching keys in both tables.
                raw_sql_plan_metrics_df = raw_sql_plan_metrics_df.merge(
                    node_level_supp,
                    left_on=['appId', 'sqlID', 'nodeID'],
                    right_on=['App ID', 'SQL ID', 'SQL Node Id'],
                    how='inner',
                )
                raw_sql_plan_metrics_df = raw_sql_plan_metrics_df.drop(
                    columns=['App ID', 'SQL ID', 'SQL Node Id']
                )
                raw_sql_plan_metrics_df['stageIds'] = raw_sql_plan_metrics_df['stageIds'].apply(
                    lambda x: str(x).split(',')
                )
                raw_sql_plan_metrics_df = raw_sql_plan_metrics_df.explode('stageIds')
                # compute supported stages for use below. TBD.
                # rename column from 'Exec Is Supported' to 'Stage Is Supported' and change elsewhere
                stages_supp = (
                    raw_sql_plan_metrics_df.groupby(['appId', 'sqlID', 'stageIds'])[
                        'Exec Is Supported'
                    ]
                    .agg('all')
                    .reset_index()
                )
                # the stageIds column resulting from the explode is of type object. So, we need to
                # convert it back to Int64. Then we drop the rows with N/A Values.
                stages_supp['stageIds'] = pd.to_numeric(stages_supp['stageIds'], errors='coerce').astype('Int64')
                # Filter out N/A: make sure to use `notna()` to handle all sort of N/A values.
                stages_supp = stages_supp.loc[
                    stages_supp['stageIds'].notna()
                ].reset_index(drop=True)
                # filter sql ops to have only supported ones for processing in calling fn
                raw_sql_plan_metrics_df = raw_sql_plan_metrics_df.loc[
                    raw_sql_plan_metrics_df['Exec Is Supported']
                ].drop(columns=['Exec Is Supported'])
            elif qualtool_filter == 'sqlId':
                sql_level_supp = (
                    node_level_supp.groupby(['App ID', 'SQL ID'])['Exec Is Supported']
                    .agg('all')
                    .reset_index()
                )
                sql_level_supp = sql_level_supp.loc[sql_level_supp['Exec Is Supported']]
                raw_sql_agg_metrics_df = raw_tbls.reports.get('coreRawSqlLevelAggregatedTaskMetricsCSV')
                if valid_df(raw_sql_agg_metrics_df):
                    raw_sql_agg_metrics_df = raw_sql_agg_metrics_df.merge(
                        sql_level_supp,
                        left_on=['appId', 'sqlID'],
                        right_on=['App ID', 'SQL ID'],
                        how='inner'
                    )
                    raw_sql_agg_metrics_df = raw_sql_agg_metrics_df.drop(
                        columns=['Exec Is Supported', 'App ID', 'SQL ID']
                    )
            else:
                # Don't filter out unsupported ops
                pass

    # sql ids to drop due to failures meeting below criteria
    # mark for removal from modeling sql ids that have failed stage time > 10% total stage time
    allowed_failed_duration_fraction = 0.10
    sqls_to_drop = pd.DataFrame(columns=['appId', 'sqlID']).astype({
        'appId': Utilities.scala_to_pandas_type('String'),
        'sqlID': Utilities.scala_to_pandas_type('Long')
    })

    ###############################################
    # coreRawStageLevelAggregatedTaskMetricsCSV
    ###############################################
    raw_stage_agg_metrics_df = raw_tbls.reports.get('coreRawStageLevelAggregatedTaskMetricsCSV')
    if valid_df(raw_stage_agg_metrics_df):
        # get the failed stage_df
        raw_failed_stage_df = raw_tbls.reports.get('coreRawFailedStagesCSV')
        if valid_df(raw_failed_stage_df) and valid_df(raw_sql_to_stage_df):
            stage_agg_tbl = raw_stage_agg_metrics_df[['appId', 'stageId', 'Duration']].merge(
                raw_sql_to_stage_df, left_on=['appId', 'stageId'], right_on=['appId', 'stageId']
            )
            total_stage_time = (
                stage_agg_tbl[['appId', 'sqlID', 'Duration']]
                .groupby(['appId', 'sqlID'])
                .agg('sum')
                .reset_index()
            )
            failed_stage_time = (
                stage_agg_tbl[['appId', 'sqlID', 'stageId', 'Duration']]
                .merge(raw_failed_stage_df, left_on=['appId', 'stageId'], right_on=['appId', 'stageId'], how='inner')[
                    ['appId', 'sqlID', 'Duration']
                ]
                .groupby(['appId', 'sqlID'])
                .agg('sum')
                .reset_index()
            )
            stage_times = total_stage_time.merge(
                failed_stage_time, on=['appId', 'sqlID'], how='inner'
            )
            # append the failed sqls to the sqls_to_drop DataFrame
            rows_to_drop = stage_times.loc[
                stage_times.Duration_y
                > stage_times.Duration_x * allowed_failed_duration_fraction
                ][['appId', 'sqlID']]
            sqls_to_drop = pd.concat([sqls_to_drop, rows_to_drop], ignore_index=True)
            # we can debug here to see the failed sqls

        if valid_df(stages_supp, allow_empty=True) and (qualtool_filter == 'stage'):
            raw_stage_agg_metrics_df = raw_stage_agg_metrics_df.merge(
                stages_supp,
                left_on=['appId', 'stageId'],
                right_on=['appId', 'stageIds'],
                how='inner'
            ).drop(columns=['stageIds'])  # drop the stageIds column from the stages_supp dataframe

        # add a boolean column to indicate if the stage is associated with a SQL ID or not.
        raw_stage_agg_metrics_df['hasSqlID'] = raw_stage_agg_metrics_df['sqlID'].notna()

    ###############################################
    # coreRawJobInformationCSV
    ###############################################
    raw_job_info_df = raw_tbls.reports.get('coreRawJobInformationCSV')
    if valid_df(raw_job_info_df):
        # dF <jobID | stageIds | sqlID | jobStartTime_min | endTime]>
        raw_job_info_df = raw_job_info_df.rename(columns={'startTime': 'jobStartTime_min'})
        raw_job_info_df['sqlID'] = raw_job_info_df['sqlID'].fillna(-1).astype(Utilities.scala_to_pandas_type('Int'))
        # TODO: Maybe we should not need this line, but it is used in the legacy code.
        # raw_job_info_df['jobID'] = 'job_' + raw_job_info_df['jobID'].astype(Utilities.scala_to_pandas_type('String'))

    ###############################################
    # coreRawJobLevelAggregatedTaskMetricsCSV
    ###############################################
    # TODO: raw_job_agg_metrics_df does not seem to be used after that. Maybe we do not need it?
    raw_job_agg_metrics_df = raw_tbls.reports.get('coreRawJobLevelAggregatedTaskMetricsCSV')
    if valid_df(raw_job_agg_metrics_df, allow_empty=True):
        # # rename the column jobId to jobID
        # raw_job_agg_metrics_df = raw_job_info_df.rename(columns={'jobId': 'jobID'})
        # get the information from jobInfo tbl
        if valid_df(raw_job_info_df, allow_empty=True):
            raw_job_agg_metrics_df = raw_job_agg_metrics_df.merge(
                raw_job_info_df[['appId', 'jobID', 'sqlID', 'jobStartTime_min']],
                left_on=['appId', 'jobId'],
                right_on=['appId', 'jobID'],
                how='left'
            )
            # Add a boolean column to indicate if the job is associated with a SQL ID or not.
            raw_job_agg_metrics_df['hasSqlID'] = raw_job_agg_metrics_df['sqlID'] != -1
            # drop redundant columns jobId
            raw_job_agg_metrics_df = raw_job_agg_metrics_df.drop(columns=['jobId'])

    raw_spark_props_df = raw_tbls.reports.get('coreRawSparkPropertiesCSV')
    if valid_df(raw_spark_props_df):
        raw_spark_props_df = raw_spark_props_df.set_index('propertyName')

    ###############################################
    # coreRawFailedTasksCSV
    ###############################################
    raw_failed_tasks_df = raw_tbls.reports.get('coreRawFailedTasksCSV')
    if valid_df(raw_failed_tasks_df):
        # aggregate failed tasks per appName, appId, sqlID
        raw_failed_tasks_df = raw_failed_tasks_df.groupby(['appId', 'stageId'])['attempt'].count().reset_index()
        raw_failed_tasks_df = raw_failed_tasks_df.merge(
            raw_sql_to_stage_df[['appId', 'stageId', 'sqlID']],
            on=['appId', 'stageId']
        )
        raw_failed_tasks_df = raw_failed_tasks_df.groupby(
            ['appId', 'sqlID']
        )['attempt'].sum().reset_index()
        raw_failed_tasks_df = raw_failed_tasks_df.rename(columns={'attempt': 'failed_tasks'})
    else:
        raw_failed_tasks_df = pd.DataFrame(columns=['appId', 'sqlID', 'failed_tasks']).astype({
            'appId': Utilities.scala_to_pandas_type('String'),
            'sqlID': Utilities.scala_to_pandas_type('Long'),
            'failed_tasks': Utilities.scala_to_pandas_type('Int')
        })

    ###############################################
    # Merging tables together
    ###############################################

    # merged app_info_tbl
    raw_exec_info_df = raw_tbls.reports.get('coreRawExecutorInformationCSV')
    if all(valid_df(t_df) for t_df in [
        app_info_df, raw_exec_dur_df, raw_sql_agg_metrics_df, raw_exec_info_df
    ]):
        # merge all the tables together
        app_info_mg = app_info_df.merge(
            raw_exec_info_df,
            left_on='appId',
            right_on='appId'
        )
        app_info_mg = app_info_mg.merge(
            raw_sql_agg_metrics_df, left_on='appId', right_on='appId'
        )
        app_info_mg = app_info_mg.merge(
            raw_exec_dur_df[
                [
                    'appId',
                    'sqlID',
                    'appDuration',
                ]
            ],
            left_on=['appId', 'sqlID'],
            right_on=['appId', 'sqlID'],
        )

        # filter out sqlIDs with aborted jobs (these are jobs failed due to sufficiently many (configurable) failed
        # attempts of a stage due to error conditions). These are failed sqlIDs that we shouldn't model,
        # but are still included in profiler output.
        raw_failed_jobs_df = raw_tbls.reports.get('coreRawFailedJobsCSV')
        if valid_df(raw_failed_jobs_df) and valid_df(raw_job_info_df):
            aborted_jobs = raw_failed_jobs_df.loc[
                raw_failed_jobs_df.failureReason.str.contains('aborted')
            ][['appId', 'jobID']]
            # TODO: Is this always empty? jobInfo contains failed jobs?
            aborted_jobs_sql_id = raw_job_info_df.merge(
                aborted_jobs, how='inner', on=['appId', 'jobID']
            )
            aborted_sql_ids = aborted_jobs_sql_id[['appId', 'sqlID']].drop_duplicates()
            sqls_to_drop = pd.concat([sqls_to_drop, aborted_sql_ids], ignore_index=True).drop_duplicates()

    else:
        app_info_mg = pd.DataFrame()

    if remove_failed_sql and valid_df(app_info_mg) and valid_df(sqls_to_drop):
        # removes rows from the app_info_mg DataFrame that have a matching appId and sqlID in
        # the sqls_to_drop DataFrame.
        # 1. performs a left merge of app_info_mg with sqls_to_drop on appId and sqlID, adding
        #    a column _merge that indicates whether each row was matched (both) or only present
        #   in app_info_mg (left_only).
        # 2. It filters the merged DataFrame to keep only rows where _merge is left_only, i.e.,
        #    those not present in sqls_to_drop.
        # 3. It drops the _merge column, returning a DataFrame with the unwanted rows removed.
        app_info_mg = app_info_mg.merge(
                            sqls_to_drop,
                            on=['appId', 'sqlID'],
                            how='left',
                            indicator=True
                        )
        app_info_mg = app_info_mg[app_info_mg['_merge'] == 'left_only'].drop(columns=['_merge'])

    return {
        'app_tbl': app_info_mg,
        'ops_tbl': raw_sql_plan_metrics_df,
        'spark_props_tbl': raw_spark_props_df,
        'job_map_tbl': raw_job_info_df,
        'job_stage_agg_tbl': raw_stage_agg_metrics_df,
        'wholestage_tbl': raw_tbls.reports.get('coreRawWholeStageCSV'),
        'ds_tbl': raw_tbls.reports.get('coreRawDataSourceInformationCSV'),
        'failed_tasks_tbl': raw_failed_tasks_df
    }


def extract_raw_features(
        ds_name: str,
        res_hs: List[ToolResultHandlerT],
        *,
        qualtool_filter: Optional[str],
        remove_failed_sql: bool = True) -> pd.DataFrame:
    """
    Extract raw features from the result handlers for a given dataset.
    :param ds_name:
    :param res_hs:
    :param qualtool_filter: Type of filter to apply to the qualification tool output, either 'stage' or None.
    :param remove_failed_sql:
    :return:
    """
    raw_tbls = load_csv_files(ds_name, res_hs)
    # if no reports were loaded, return an empty DataFrame
    if not raw_tbls.reports:
        return pd.DataFrame()
    # process the raw features from the loaded CSV files
    process_raw_features(
        raw_tbls,
        qualtool_filter=qualtool_filter,
        remove_failed_sql=remove_failed_sql
    )
    print(f'those are the successful loaded reports: {raw_tbls.reports.keys()}')
    return pd.DataFrame()
