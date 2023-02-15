# Copyright (c) 2023, NVIDIA CORPORATION.
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

"""Implementation class representing wrapper around the RAPIDS acceleration Qualification tool."""

import dataclasses
import json
from dataclasses import dataclass
from typing import Any, List

import pandas as pd
from tabulate import tabulate

from spark_rapids_pytools.cloud_api.sp_types import ClusterBase, EnumeratedType
from spark_rapids_pytools.common.sys_storage import FSUtil
from spark_rapids_pytools.common.utilities import Utils
from spark_rapids_pytools.pricing.price_provider import SavingsEstimator
from spark_rapids_pytools.rapids.rapids_job import RapidsJobPropContainer
from spark_rapids_pytools.rapids.rapids_tool import RapidsJarTool


class QualFilterApp(EnumeratedType):
    """Values used to filter out the applications in the qualification report"""
    SAVINGS = 'savings'
    SPEEDUPS = 'speedups'
    NONE = 'none'


@dataclass
class QualificationSummary:
    """
    Encapsulates the logic to organize Qualification report.
    """
    comments: Any = None
    all_apps: pd.DataFrame = None
    recommended_apps: pd.DataFrame = None
    df_result: pd.DataFrame = None

    def _get_total_durations(self) -> int:
        if not self.is_empty():
            return self.all_apps['App Duration'].sum()
        return 0

    def _get_total_gpu_durations(self) -> int:
        if not self.is_empty():
            return self.all_apps['Estimated GPU Duration'].sum()
        return 0

    def _get_stats_total_cost(self) -> float:
        return self.df_result['Estimated App Cost'].sum()

    def _get_stats_total_gpu_cost(self) -> float:
        return self.df_result['Estimated GPU Cost'].sum()

    def _get_stats_total_apps(self) -> int:
        if not self.is_empty():
            return len(self.all_apps)
        return 0

    def _get_stats_recommended_apps(self) -> int:
        if self.has_gpu_recommendation():
            return len(self.recommended_apps)
        return 0

    def is_empty(self) -> bool:
        if self.all_apps is not None:
            return self.all_apps.empty
        return True

    def has_gpu_recommendation(self) -> bool:
        if self.recommended_apps is not None:
            return not self.recommended_apps.empty
        return False

    def has_tabular_result(self) -> bool:
        if self.df_result is not None:
            return not self.df_result.empty
        return False

    def generate_report(self,
                        app_name: str,
                        wrapper_csv_file: str = None,
                        config_provider=None,
                        df_pprinter: Any = None,
                        output_pprinter: Any = None):
        def format_float(x: float) -> str:
            return f'{x:.2f}'

        report_content = []

        if self.is_empty():
            # Qualification tool has no output
            report_content.append(f'{app_name} tool did not generate any valid rows')
            if self.comments is not None and len(self.comments) > 0:
                report_content.append('\n'.join(self.comments))
            return report_content

        if output_pprinter is not None:
            report_content.append(output_pprinter())

        if not self.has_gpu_recommendation():
            report_content.append(f'{app_name} tool found no recommendations for GPU.')

        if self.has_tabular_result():
            if wrapper_csv_file is not None:
                abs_path = FSUtil.get_abs_path(wrapper_csv_file)
                report_content.append(f'\t- Full savings and speedups CSV report: {abs_path}')

            pretty_df = df_pprinter(self.df_result)
            if pretty_df.empty:
                # the results were reduced to no rows because of the filters
                report_content.append(
                    f'{app_name} tool found no qualified applications after applying the filters.\n'
                    f'See the CSV file for full report or disable the filters.')
            else:
                report_content.append(tabulate(pretty_df, headers='keys', tablefmt='psql', floatfmt='.2f'))
        else:
            report_content.append(f'{app_name} tool found no records to show.')

        total_app_cost = self._get_stats_total_cost()
        total_gpu_cost = self._get_stats_total_gpu_cost()
        estimated_gpu_savings = 0.0
        if total_app_cost > 0.0:
            estimated_gpu_savings = 100.0 - (100.0 * total_gpu_cost / total_app_cost)
        overall_speedup = 0.0
        total_apps_durations = 1.0 * self._get_total_durations()
        total_gpu_durations = self._get_total_gpu_durations()
        if total_gpu_durations > 0:
            overall_speedup = total_apps_durations / total_gpu_durations
        report_content.append('Report Summary:')
        report_summary = [['Total applications', self._get_stats_total_apps()],
                          ['RAPIDS candidates', self._get_stats_recommended_apps()],
                          ['Overall estimated speedup', format_float(overall_speedup)],
                          ['Overall estimated cost savings', f'{format_float(estimated_gpu_savings)}%']]
        report_content.append(tabulate(report_summary, colalign=('left', 'right')))
        if self.comments is not None and len(self.comments) > 0:
            report_content.extend(f'{line}' for line in self.comments)
        if self.has_gpu_recommendation() and config_provider is not None:
            report_content.append(config_provider())
        return report_content


@dataclass
class Qualification(RapidsJarTool):
    """
    Wrapper layer around Qualification Tool.
    """
    name = 'qualification'

    def _process_rapids_args(self):
        """
        Qualification tool processes extra arguments:
        1. filter out applications.
        2. gpu-device type to be used for the cost estimation.
        3. gpu_per_machine: number of gpu installed on a worker node.
        4. cuda version
        """
        super()._process_rapids_args()
        self.logger.info('Qualification tool processing the arguments')

    def _create_migration_cluster(self, cluster_type: str, cluster_arg: str) -> ClusterBase:
        if cluster_arg is None:
            raise RuntimeError(f'The {cluster_type} cluster argument is not set.')
        arg_is_file = self.ctxt.platform.storage.is_file_path(cluster_arg)
        if not arg_is_file:
            self.logger.info('Loading %s cluster properties by name %s. Note that this will fail '
                             'if the cluster was permanently deleted.',
                             cluster_type,
                             cluster_arg)
            # create a cluster by name
            cluster_obj = self.ctxt.platform.connect_cluster_by_name(cluster_arg)
        else:
            self.logger.info('Loading %s cluster cluster properties from file %s',
                             cluster_type,
                             cluster_arg)
            # create cluster by loading properties files
            # download the file to the working directory
            cluster_conf_path = self.ctxt.platform.storage.download_resource(cluster_arg,
                                                                             self.ctxt.get_local_work_dir())
            cluster_obj = self.ctxt.platform.load_cluster_by_prop_file(cluster_conf_path)
        return cluster_obj

    def _process_cpu_cluster_args(self, offline_cluster_opts: dict = None):
        # get the name of the cpu_cluster
        cpu_cluster_arg = offline_cluster_opts.get('cpuCluster')
        cpu_cluster_obj = self._create_migration_cluster('CPU', cpu_cluster_arg)
        self.ctxt.set_ctxt('cpuClusterProxy', cpu_cluster_obj)

    def _process_gpu_cluster_args(self, offline_cluster_opts: dict = None):
        gpu_cluster_arg = offline_cluster_opts.get('gpuCluster')
        if gpu_cluster_arg is None:
            self.logger.info('Creating GPU cluster by converting the CPU cluster instances to GPU supported types')
            # Convert the CPU instances to support gpu
            orig_cluster = self.ctxt.get_ctxt('cpuClusterProxy')
            gpu_cluster_obj = self.ctxt.platform.migrate_cluster_to_gpu(orig_cluster)
        else:
            gpu_cluster_obj = self._create_migration_cluster('GPU', gpu_cluster_arg)
        self.ctxt.set_ctxt('gpuClusterProxy', gpu_cluster_obj)

    def _process_offline_cluster_args(self):
        offline_cluster_opts = self.wrapper_options.get('migrationClustersProps', {})
        self._process_cpu_cluster_args(offline_cluster_opts)
        self._process_gpu_cluster_args(offline_cluster_opts)

    def __process_filter_args(self, arg_val: str):
        available_filters = [filter_enum.value for filter_enum in QualFilterApp]
        default_filter_txt = self.ctxt.get_value('sparkRapids', 'cli', 'defaults', 'filters',
                                                 'defaultFilter')
        if arg_val is not None:
            try:
                selected_filter = QualFilterApp.fromstring(arg_val)
            except Exception:  # pylint: disable=broad-except
                selected_filter = QualFilterApp.fromstring(default_filter_txt)
                self.logger.warning(
                    'Invalid argument filter_apps=%s.\n\t'
                    'Accepted options are: [%s].\n\t'
                    'Falling-back to default filter: %s',
                    arg_val, ' | '.join(available_filters), default_filter_txt)
        else:
            selected_filter = QualFilterApp.fromstring(default_filter_txt)
        self.ctxt.set_ctxt('filterApps', selected_filter)

    def _process_eventlogs_args(self):
        eventlog_arg = self.wrapper_options.get('eventlogs')
        if eventlog_arg is None:
            # get the eventlogs from spark properties
            cpu_cluster_obj = self.ctxt.get_ctxt('cpuClusterProxy')
            spark_event_logs = cpu_cluster_obj.get_eventlogs_from_config()
        else:
            if isinstance(eventlog_arg, tuple):
                spark_event_logs = List[eventlog_arg]
            elif isinstance(eventlog_arg, str):
                spark_event_logs = eventlog_arg.split(',')
            else:
                spark_event_logs = eventlog_arg
        if len(spark_event_logs) < 1:
            self.logger.error('Eventlogs list is empty. '
                              'The cluster Spark properties may be missing "spark.eventLog.dir". '
                              'Re-run the command passing "--eventlogs" flag to the wrapper.')
            raise RuntimeError('Invalid arguments. The list of Apache Spark event logs is empty.')
        self.ctxt.set_ctxt('eventLogs', spark_event_logs)

    def _process_custom_args(self):
        """
        Qualification tool processes extra arguments:
        1. filter out applications.
        2. gpu-device type to be used for the cost estimation.
        3. gpu_per_machine: number of gpu installed on a worker node.
        4. cuda version
        """
        gpu_device = self.ctxt.get_value('sparkRapids', 'gpu', 'device')
        gpu_device_arg = self.wrapper_options.get('gpuDevice')
        if gpu_device_arg is not None:
            gpu_device = gpu_device_arg
        gpu_per_machine = int(self.ctxt.get_value('sparkRapids', 'gpu', 'workersPerNode'))
        gpu_per_machine_arg = self.wrapper_options.get('gpuPerMachine')
        if gpu_per_machine_arg is not None:
            gpu_per_machine = gpu_per_machine_arg
        cuda = self.ctxt.get_value('sparkRapids', 'gpu', 'cudaVersion')
        cuda_arg = self.wrapper_options.get('cuda')
        if cuda_arg is not None:
            cuda = cuda_arg
        self.ctxt.set_ctxt('gpuPerMachine', gpu_per_machine)
        self.ctxt.set_ctxt('gpuDevice', gpu_device)
        self.ctxt.set_ctxt('cuda', cuda)
        self.__process_filter_args(self.wrapper_options.get('filterApps'))
        self._process_offline_cluster_args()
        self._process_eventlogs_args()
        # This is noise to dump everything
        # self.logger.debug('%s custom arguments = %s', self.pretty_name(), self.ctxt.props['wrapperCtx'])

    def _process_job_submission_args(self):
        job_args = {}
        submission_args = self.wrapper_options.get('jobSubmissionProps')
        # get the root remote folder and make sure it exists
        remote_folder = submission_args.get('remoteFolder')
        # TODO verify the remote folder is correct
        if not self.ctxt.platform.storage.resource_exists(remote_folder):
            raise RuntimeError(f'Remote folder [{remote_folder}] is invalid.')
        # now we should make the subdirectory to indicate the output folder,
        # by appending the name of the execution folder
        exec_full_name = self.ctxt.get_ctxt('execFullName')
        remote_workdir = FSUtil.build_url_from_parts(remote_folder, exec_full_name)
        self.ctxt.set_remote('rootFolder', remote_folder)
        self.ctxt.set_remote('workDir', remote_workdir)
        self.logger.info('Remote workdir is set as %s', remote_workdir)
        remote_dep_folder = FSUtil.build_url_from_parts(remote_workdir, self.ctxt.get_ctxt('depFolderName'))
        self.ctxt.set_remote('depFolder', remote_dep_folder)
        self.logger.info('Remote dependency folder is set as %s', remote_dep_folder)
        job_args['remoteFolder'] = remote_workdir
        platform_args = submission_args.get('platformArgs')
        if platform_args is not None:
            processed_platform_args = self.ctxt.platform.validate_job_submission_args(platform_args)
            job_args['platformArgs'] = processed_platform_args
        self.ctxt.update_job_args(job_args)

    def _copy_dependencies_to_remote(self):
        self.logger.info('Preparing remote dependency folder')
        remote_work_dir = self.ctxt.get_remote('workDir')
        local_folder = self.ctxt.get_local('outputFolder')
        cp_res = self.ctxt.platform.storage.upload_resource(local_folder, remote_work_dir)
        self.logger.debug('Executed command of copying %s', cp_res)

    def _prepare_job_arguments(self):
        job_args = self.ctxt.get_ctxt('jobArgs')
        remote_folder = job_args.get('remoteFolder')
        if remote_folder is None:
            # for dataproc we can get the tmp gs storage
            self.logger.info('The remote directory to archive the job results is not set')
        else:
            # check the remote_folder exists
            if not self.ctxt.platform.storage.resource_exists(remote_folder):
                raise RuntimeError(f'Remote folder [{remote_folder}] does not exist.')
        # now we can create the job object
        # Todo: For dataproc, this can be autogenerated from cluster name
        rapids_arg_list = []
        ctxt_rapids_args = self.ctxt.get_ctxt('rapidsArgs')
        jar_file_name = ctxt_rapids_args.get('jarFileName')
        rapids_opts = ctxt_rapids_args.get('rapidsOpts')
        if rapids_opts is not None:
            rapids_arg_list.extend(rapids_opts)
        # add the eventlogs at the end of all the tool options
        rapids_arg_list.extend(self.ctxt.get_ctxt('eventLogs'))
        class_name = self.ctxt.get_value('sparkRapids', 'mainClass')
        remote_jar = FSUtil.build_url_from_parts(self.ctxt.get_remote('depFolder'), jar_file_name)
        rapids_arg_obj = {
            'jarFile': remote_jar,
            'jarArgs': rapids_arg_list,
            'className': class_name
        }
        # EMR specific things
        platform_args = job_args.get('platformArgs')
        spark_conf_args = {
            'properties': {
                'spark.executor.cores': '4',
                'spark.executor.memory': '20g',
                'spark.driver.cores': '4',
                'spark.driver.memory': '8g',
                'spark.executor.instances': '1'
            }
        }
        job_properties_json = {
            'remoteOutput': remote_folder,
            'rapidsArgs': rapids_arg_obj,
            'sparkConfArgs': spark_conf_args,
            'platformArgs': platform_args
        }
        job_properties = RapidsJobPropContainer(prop_arg=json.dumps(job_properties_json),
                                                file_load=False)
        job_obj = self.ctxt.platform.create_submission_job(job_prop=job_properties, ctxt=self.ctxt)
        job_obj.run_job()

    def _run_rapids_tool(self):
        # 1- copy dependencies to remote server
        self._copy_dependencies_to_remote()
        # 2- prepare the arguments
        #  2.a -check if the app_id is not none
        self._prepare_job_arguments()
        #
        # 3- create a submission job
        # 4- execute

    def __get_recommended_apps(self, all_rows, selected_cols=None) -> pd.DataFrame:
        speed_up_col = self.ctxt.get_value('toolOutput', 'csv', 'summaryReport',
                                           'recommendations', 'speedUp', 'columnName')
        recommended_vals = self.ctxt.get_value('toolOutput', 'csv', 'summaryReport',
                                               'recommendations', 'speedUp', 'selectedRecommendations')
        mask = all_rows[speed_up_col].isin(recommended_vals)
        if selected_cols is None:
            return all_rows.loc[mask]
        return all_rows.loc[mask, selected_cols]

    def __remap_columns_and_prune(self, all_rows) -> pd.DataFrame:
        cols_subset = self.ctxt.get_value('toolOutput', 'csv', 'summaryReport', 'columns')
        cols_map = self.ctxt.get_value('toolOutput', 'csv', 'summaryReport', 'mapColumns')
        subset_data = all_rows.loc[:, cols_subset]

        for col_rename in cols_map:
            subset_data.columns = subset_data.columns.str.replace(col_rename,
                                                                  cols_map.get(col_rename),
                                                                  regex=False)
        return subset_data

    def _report_tool_full_location(self) -> str:
        out_folder_path = self.ctxt.get_rapids_output_folder()
        res_arr = [Utils.gen_str_header('Output'),
                   f'\t{self.pretty_name()} tool output: {out_folder_path}']
        subfiles = FSUtil.get_all_files(out_folder_path)
        if len(subfiles) > 0:
            res_arr.append(f'\t{FSUtil.get_resource_name(out_folder_path)}/')
            for sub_file in subfiles:
                if self.ctxt.platform.storage.resource_is_dir(sub_file):
                    leaf_name = f'└── {FSUtil.get_resource_name(sub_file)}/'
                else:
                    leaf_name = f'├── {FSUtil.get_resource_name(sub_file)}'
                if '$folder$' not in leaf_name:
                    # this a metafile created in S3 that we do not need
                    res_arr.append(f'\t\t{leaf_name}')
            doc_url = self.ctxt.get_value('sparkRapids', 'outputDocURL')
            res_arr.append(f'\t- To learn more about the output details, visit '
                           f'{doc_url}')
            return '\n'.join(res_arr)
        return None

    def __generate_mc_types_conversion_report(self):
        report_content = []
        if bool(self.ctxt.platform.ctxt['notes']):
            # get the converted instance types
            node_conversions = self.ctxt.platform.ctxt['notes'].get('nodeConversions')
            if node_conversions is not None:
                report_content = [
                    'Instance types conversions:',
                ]
                conversion_items = []
                for mc_src, mc_target in node_conversions.items():
                    conversion_items.append([mc_src, 'to', mc_target])
                report_content.append(tabulate(conversion_items))
                report_content.append('To support acceleration with T4 GPUs, switch the worker node '
                                      'instance types.')
        return report_content

    def __build_global_report_summary(self,
                                      all_apps: pd.DataFrame,
                                      csv_out: str) -> QualificationSummary:
        def get_costs_for_single_app(df_row, estimator: SavingsEstimator) -> pd.Series:
            est_cpu_cost, est_gpu_cost, est_savings = estimator.get_costs_and_savings(df_row['App Duration'],
                                                                                      df_row['Estimated GPU Duration'])
            if est_savings < 1.0:
                savings_recommendations = 'Not Recommended'
            elif est_savings < 40.0:
                savings_recommendations = 'Recommended'
            else:
                savings_recommendations = 'Strongly Recommended'
            return pd.Series([savings_recommendations, est_cpu_cost, est_gpu_cost, est_savings])

        # initialize the savings estimator
        savings_estimator = self.ctxt.platform.create_saving_estimator(
            self.ctxt.get_ctxt('cpuClusterProxy'),
            self.ctxt.get_ctxt('gpuClusterProxy'))
        extra_comments = savings_estimator.comments
        conversion_comments = self.__generate_mc_types_conversion_report()
        extra_comments.extend(conversion_comments)

        if all_apps.empty:
            return QualificationSummary(comments=extra_comments)
        cost_cols = self.ctxt.get_value('local', 'output', 'costColumns')
        cost_rec_col = self.ctxt.get_value('local', 'output', 'savingRecommendColumn')
        # rename recommendation column to another name
        speed_recom_col = self.ctxt.get_value('local', 'output', 'speedupRecommendColumn')
        apps_working_set = self.__remap_columns_and_prune(all_apps)
        recommended_apps = self.__get_recommended_apps(apps_working_set)

        apps_working_set[cost_cols] = apps_working_set.apply(
            lambda row: get_costs_for_single_app(row, estimator=savings_estimator), axis=1)
        apps_working_set.loc[apps_working_set[speed_recom_col] == 'Not Applicable', cost_rec_col] = 'Not Applicable'
        if not apps_working_set.empty:
            self.logger.info('Generating GPU Estimated Speedup and Savings as %s', csv_out)
            apps_working_set.to_csv(csv_out)
        return QualificationSummary(comments=extra_comments,
                                    all_apps=all_apps,
                                    recommended_apps=recommended_apps,
                                    df_result=apps_working_set)

    def _process_output(self):
        def process_df_for_stdout(raw_df):
            """
            process the dataframe to be more readable on the stdout
            1- convert time durations to second
            2- shorten headers
            """
            selected_cols = self.ctxt.get_value('local', 'output', 'summaryColumns')
            # check if any filters apply
            filter_recom_enabled = self.ctxt.get_ctxt('filterApps') == QualFilterApp.SPEEDUPS
            filter_pos_enabled = self.ctxt.get_ctxt('filterApps') == QualFilterApp.SAVINGS
            # filter by recommendations if enabled
            if filter_recom_enabled:
                df_row = self.__get_recommended_apps(raw_df, selected_cols)
            else:
                df_row = raw_df.loc[:, selected_cols]
            if df_row.empty:
                return df_row
            # filter by savings if enabled
            if filter_pos_enabled:
                saving_cost_col = self.ctxt.get_value('local', 'output', 'savingRecommendColumn')
                recommended_vals = self.ctxt.get_value('toolOutput', 'csv', 'summaryReport',
                                                       'recommendations', 'speedUp',
                                                       'selectedRecommendations')
                cost_mask = df_row[saving_cost_col].isin(recommended_vals)
                df_row = df_row.loc[cost_mask, selected_cols]
                if df_row.empty:
                    self.ctxt.set_ctxt('wrapper_output_content',
                                       'Found no qualified apps for cost savings.')
                    return df_row
            time_unit = '(ms)'
            time_from_conf = self.ctxt.get_value('toolOutput', 'stdout', 'summaryReport', 'timeUnits')
            if time_from_conf == 's':
                time_unit = '(s)'
                # convert to seconds
                for column in df_row[[col for col in df_row.columns if 'Duration' in col]]:
                    df_row[column] = df_row[column].div(1000).round(2)
            # change the header to include time unit
            df_row.columns = df_row.columns.str.replace('Duration',
                                                        f'Duration{time_unit}', regex=False)
            # squeeze the header titles if enabled
            if self.ctxt.get_value('toolOutput', 'stdout', 'summaryReport', 'compactWidth'):
                for column in df_row.columns:
                    if len(column) > 10:
                        split_ch = ' '
                        split_pos = column.rfind(split_ch)
                        if split_pos > -1:
                            new_column_name = column[:split_pos] + '\n' + column[split_pos + len(split_ch):]
                            df_row.columns = df_row.columns.str.replace(column,
                                                                        new_column_name, regex=False)
            return df_row

        rapids_output_dir = self.ctxt.get_rapids_output_folder()
        if not self.ctxt.platform.storage.resource_exists(rapids_output_dir):
            self.ctxt.set_ctxt('wrapper_output_content',
                               self._report_results_are_empty())
            return
        rapids_summary_file = FSUtil.build_path(rapids_output_dir,
                                                self.ctxt.get_value('toolOutput', 'csv', 'summaryReport', 'fileName'))
        self.ctxt.logger.debug('Rapids CSV summary file is located as: %s', rapids_summary_file)
        df = pd.read_csv(rapids_summary_file)
        csv_file_name = self.ctxt.get_value('local', 'output', 'fileName')
        csv_summary_file = FSUtil.build_path(self.ctxt.get_output_folder(), csv_file_name)
        report_gen = self.__build_global_report_summary(df, csv_summary_file)
        summary_report = report_gen.generate_report(app_name=self.pretty_name(),
                                                    wrapper_csv_file=csv_summary_file,
                                                    config_provider=None,
                                                    df_pprinter=process_df_for_stdout,
                                                    output_pprinter=self._report_tool_full_location)
        self.ctxt.set_ctxt('wrapper_output_content', summary_report)

    def _write_summary(self):
        wrapper_out_content = self.ctxt.get_ctxt('wrapper_output_content')
        if wrapper_out_content is not None:
            if isinstance(wrapper_out_content, list):
                print('\n'.join(wrapper_out_content))

    def _archive_results(self):
        remote_work_dir = self.ctxt.get_remote('workDir')
        if remote_work_dir is not None:
            local_folder = self.ctxt.get_output_folder()
            # TODO make sure it worth issuing the command
            rapids_subfolder = self.ctxt.get_value_silent('toolOutput', 'subFolder')
            exclude_folder = rapids_subfolder
            self.ctxt.platform.storage.upload_resource(local_folder,
                                                       remote_work_dir,
                                                       exclude_pattern=exclude_folder)


@dataclasses.dataclass
class QualificationAsLocal(Qualification):
    """
    Qualification tool running on local development.
    """
    description: str = 'This is the localQualification'

    def _copy_dependencies_to_remote(self):
        self.logger.info('Skipping preparing remote dependency folder')

    def _process_job_submission_args(self):
        def validate_env_runs(submit_args: dict) -> dict:
            aws_access_id = self.ctxt.platform.cli.get_env_var('aws_access_key_id')
            aws_access_key = self.ctxt.platform.cli.get_env_var('aws_secret_access_key')
            jvm_heap_size = submit_args.get('jvmMaxHeapSize')
            xmx_key = f'Xmx{jvm_heap_size}g'
            ctxt_rapids_args = self.ctxt.get_ctxt('rapidsArgs')
            dependencies = ctxt_rapids_args.get('javaDependencies')
            res = {
                'jvmArgs': {
                    # TODO setting the AWS access keys from jvm arguments did not work
                    # 'Dspark.hadoop.fs.s3a.secret.key': aws_access_key,
                    # 'Dspark.hadoop.fs.s3a.access.key': aws_access_id
                    xmx_key: ''
                },
                'envArgs': {
                    'AWS_ACCESS_KEY_ID': aws_access_id,
                    'AWS_SECRET_ACCESS_KEY': aws_access_key
                },
                'dependencies': dependencies
            }
            return res

        job_args = {}
        submission_args = self.wrapper_options.get('jobSubmissionProps')
        # get the root remote folder and make sure it exists
        remote_folder = submission_args.get('remoteFolder')
        # If remote_folder is not specified, then ignore it
        if remote_folder is None:
            # the output is only for local machine
            self.logger.info('No remote output folder specified.')
        else:
            # TODO verify the remote folder is correct
            if not self.ctxt.platform.storage.resource_exists(remote_folder):
                raise RuntimeError(f'Remote folder [{remote_folder}] is invalid.')
            # now we should make the subdirectory to indicate the output folder,
            # by appending the name of the execution folder
            exec_full_name = self.ctxt.get_ctxt('execFullName')
            remote_workdir = FSUtil.build_url_from_parts(remote_folder, exec_full_name)
            self.ctxt.set_remote('rootFolder', remote_folder)
            self.ctxt.set_remote('workDir', remote_workdir)
            self.logger.info('Remote workdir is set as %s', remote_workdir)
            remote_dep_folder = FSUtil.build_url_from_parts(remote_workdir,
                                                            self.ctxt.get_ctxt('depFolderName'))
            self.ctxt.set_remote('depFolder', remote_dep_folder)
            self.logger.info('Remote dependency folder is set as %s', remote_dep_folder)
        # the output folder has to be set any way
        job_args['outputFolder'] = self.ctxt.get_output_folder()
        platform_args = submission_args.get('platformArgs')
        if platform_args is not None:
            processed_platform_args = validate_env_runs(platform_args)
            job_args['platformArgs'] = processed_platform_args
        self.ctxt.update_job_args(job_args)

    def _prepare_job_arguments(self):
        job_args = self.ctxt.get_ctxt('jobArgs')
        output_folder = job_args.get('outputFolder')
        # now we can create the job object
        # Todo: For dataproc, this can be autogenerated from cluster name
        rapids_arg_list = []
        ctxt_rapids_args = self.ctxt.get_ctxt('rapidsArgs')
        jar_file_path = ctxt_rapids_args.get('jarFilePath')
        rapids_opts = ctxt_rapids_args.get('rapidsOpts')
        if rapids_opts is not None:
            rapids_arg_list.extend(rapids_opts)
        # add the eventlogs at the end of all the tool options
        rapids_arg_list.extend(self.ctxt.get_ctxt('eventLogs'))
        class_name = self.ctxt.get_value('sparkRapids', 'mainClass')
        rapids_arg_obj = {
            'jarFile': jar_file_path,
            'jarArgs': rapids_arg_list,
            'className': class_name
        }
        # EMR specific things
        platform_args = job_args.get('platformArgs')
        spark_conf_args = {}
        job_properties_json = {
            'remoteOutput': output_folder,
            'rapidsArgs': rapids_arg_obj,
            'sparkConfArgs': spark_conf_args,
            'platformArgs': platform_args
        }
        job_properties = RapidsJobPropContainer(prop_arg=json.dumps(job_properties_json),
                                                file_load=False)
        job_obj = self.ctxt.platform.create_local_submission_job(job_prop=job_properties,
                                                                 ctxt=self.ctxt)
        job_obj.run_job()

    def _delete_remote_dep_folder(self):
        self.logger.debug('Local mode skipping deleting the remote workdir')

    def _download_remote_output_folder(self):
        self.logger.debug('Local mode skipping downloading the remote output workdir')

    def _archive_results(self):
        remote_work_dir = self.ctxt.get_remote('workDir')
        if remote_work_dir is not None:
            local_folder = self.ctxt.get_output_folder()
            # TODO make sure it worth issuing the command
            self.ctxt.platform.storage.upload_resource(local_folder, remote_work_dir)
