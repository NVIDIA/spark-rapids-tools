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

import textwrap
from dataclasses import dataclass, field
from math import ceil
from typing import Any, List, Callable

import pandas as pd
from tabulate import tabulate

from spark_rapids_pytools.cloud_api.sp_types import EnumeratedType, ClusterReshape
from spark_rapids_pytools.common.sys_storage import FSUtil
from spark_rapids_pytools.common.utilities import Utils, TemplateGenerator
from spark_rapids_pytools.pricing.price_provider import SavingsEstimator
from spark_rapids_pytools.rapids.rapids_tool import RapidsJarTool


class QualFilterApp(EnumeratedType):
    """Values used to filter out the applications in the qualification report"""
    SAVINGS = 'savings'
    SPEEDUPS = 'speedups'
    NONE = 'none'


class QualGpuClusterReshapeType(EnumeratedType):
    """Values used to filter out the applications in the qualification report"""
    MATCH = 'match'
    CLUSTER = 'cluster'
    JOB = 'job'

    @classmethod
    def get_default(cls):
        return cls.MATCH


@dataclass
class QualificationSummary:
    """
    Encapsulates the logic to organize Qualification report.
    """
    comments: Any = None
    all_apps: pd.DataFrame = None
    recommended_apps: pd.DataFrame = None
    df_result: pd.DataFrame = None
    irrelevant_speedups: bool = False
    pricing_config: Any = None
    sections_generators: List[Callable] = field(default_factory=lambda: [])

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
                        csp_report_provider: Callable[[], List[str]] = lambda: [],
                        df_pprinter: Any = None,
                        output_pprinter: Any = None):

        def format_float(x: float) -> str:
            return f'{x:.2f}'

        report_content = []

        if self.is_empty():
            # Qualification tool has no output
            report_content.append(f'{app_name} tool did not generate any valid rows')
            if self.comments:
                report_content.append(Utils.gen_multiline_str(self.comments))
            return report_content

        if output_pprinter is not None:
            report_content.append(output_pprinter())

        if not self.has_gpu_recommendation():
            if not self.irrelevant_speedups:
                report_content.append(f'{app_name} tool found no recommendations for GPU.')

        if self.has_tabular_result():
            if wrapper_csv_file is not None:
                abs_path = FSUtil.get_abs_path(wrapper_csv_file)
                report_content.append(f'    - Full savings and speedups CSV report: {abs_path}')

            pretty_df = df_pprinter(self.df_result)
            if pretty_df.empty:
                # the results were reduced to no rows because of the filters
                report_content.append(
                    f'{app_name} tool found no qualified applications after applying the filters.\n'
                    f'See the CSV file for full report or disable the filters.')
            else:
                report_content.append(tabulate(pretty_df, headers='keys', tablefmt='psql', floatfmt='.2f'))
        elif self.pricing_config is None:
            report_content.append(f'pricing information not found for ${app_name}')
        else:
            report_content.append(f'{app_name} tool found no records to show.')

        overall_speedup = 0.0
        total_apps_durations = 1.0 * self._get_total_durations()
        total_gpu_durations = self._get_total_gpu_durations()
        if total_gpu_durations > 0:
            overall_speedup = total_apps_durations / total_gpu_durations

        if self.pricing_config is None:
            report_content.append(Utils.gen_report_sec_header('Report Summary', hrule=False))
            report_summary = [['Total applications', self._get_stats_total_apps()],
                              ['Overall estimated speedup', format_float(overall_speedup)]]
        else:
            total_app_cost = self._get_stats_total_cost()
            total_gpu_cost = self._get_stats_total_gpu_cost()
            estimated_gpu_savings = 0.0
            if total_app_cost > 0.0:
                estimated_gpu_savings = 100.0 - (100.0 * total_gpu_cost / total_app_cost)

            report_content.append(Utils.gen_report_sec_header('Report Summary', hrule=False))
            report_summary = [['Total applications', self._get_stats_total_apps()],
                              ['Overall estimated speedup', format_float(overall_speedup)],
                              ['Overall estimated cost savings', f'{format_float(estimated_gpu_savings)}%']]
        if not self.irrelevant_speedups:
            # do not display speedups stats if the speedup is being overriden by the shape recommendations
            report_summary.insert(1, ['RAPIDS candidates', self._get_stats_recommended_apps()])
        report_content.append(tabulate(report_summary, colalign=('left', 'right')))
        if self.comments:
            report_content.append(Utils.gen_report_sec_header('Notes'))
            report_content.extend(f' - {line}' for line in self.comments)
        if self.sections_generators:
            for section_generator in self.sections_generators:
                report_content.append(Utils.gen_multiline_str(section_generator()))
        if self.has_gpu_recommendation():
            csp_report = csp_report_provider()
            if csp_report:
                report_content.extend(csp_report)
        # append an empty line at the end of the report
        report_content.append('')
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
        """
        self.logger.info('Qualification tool processing the arguments')
        super()._process_rapids_args()

    def _process_cpu_cluster_args(self, offline_cluster_opts: dict = None):
        # get the name of the cpu_cluster
        cpu_cluster_arg = offline_cluster_opts.get('cpuCluster')
        if cpu_cluster_arg is not None:
            cpu_cluster_obj = self._create_migration_cluster('CPU', cpu_cluster_arg)
            self.ctxt.set_ctxt('cpuClusterProxy', cpu_cluster_obj)

    def _process_gpu_cluster_args(self, offline_cluster_opts: dict = None) -> bool:
        gpu_cluster_arg = offline_cluster_opts.get('gpuCluster')
        if gpu_cluster_arg is None:
            self.logger.info('Creating GPU cluster by converting the CPU cluster instances to GPU supported types')
            # Convert the CPU instances to support gpu
            orig_cluster = self.ctxt.get_ctxt('cpuClusterProxy')
            gpu_cluster_obj = self.ctxt.platform.migrate_cluster_to_gpu(orig_cluster)
        else:
            gpu_cluster_obj = self._create_migration_cluster('GPU', gpu_cluster_arg)
        self.ctxt.set_ctxt('gpuClusterProxy', gpu_cluster_obj)
        return True

    def _process_offline_cluster_args(self):
        offline_cluster_opts = self.wrapper_options.get('migrationClustersProps', {})
        self._process_cpu_cluster_args(offline_cluster_opts)
        self._process_gpu_cluster_args(offline_cluster_opts)

    def __process_gpu_cluster_recommendation(self, arg_val: str):
        available_types = [filter_enum.value for filter_enum in QualGpuClusterReshapeType]
        default_recommendation_txt = self.ctxt.get_value('sparkRapids', 'cli', 'defaults',
                                                         'gpuClusterRecommendation',
                                                         'defaultRecommendation')
        if arg_val:
            try:
                selected_recommendation = QualGpuClusterReshapeType.fromstring(arg_val)
            except Exception:  # pylint: disable=broad-except
                selected_recommendation = QualGpuClusterReshapeType.fromstring(default_recommendation_txt)
                self.logger.warning(
                    'Invalid argument gpu_cluster_recommendation=%s.\n\t'
                    'Accepted options are: [%s].\n\t'
                    'Falling-back to default filter: %s',
                    arg_val, Utils.gen_joined_str(' | ', available_types), default_recommendation_txt)
        else:
            selected_recommendation = QualFilterApp.fromstring(default_recommendation_txt)
        self.ctxt.set_ctxt('gpuClusterShapeRecommendation', selected_recommendation)

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
                    arg_val, Utils.gen_joined_str(' | ', available_filters), default_filter_txt)
        else:
            selected_filter = QualFilterApp.fromstring(default_filter_txt)
        if self.__recommendation_is_non_standard():
            # SpeedupFilter cannot be applied with the current cluster_gpu_recommendation
            if selected_filter == QualFilterApp.SPEEDUPS:
                self.logger.info('Cannot apply Filter argument filter_apps=%s with the selected '
                                 'gpu_cluster_shape recommendation. Setting the filter to %s',
                                 QualFilterApp.tostring(selected_filter),
                                 default_filter_txt)
                selected_filter = QualFilterApp.fromstring(default_filter_txt)
        self.ctxt.set_ctxt('filterApps', selected_filter)

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
        target_platform = self.wrapper_options.get('target_platform')
        self.ctxt.set_ctxt('targetPlatform', target_platform)
        self.ctxt.set_ctxt('gpuPerMachine', gpu_per_machine)
        self.ctxt.set_ctxt('gpuDevice', gpu_device)
        self.ctxt.set_ctxt('cuda', cuda)
        # we need to process each argument to verify it is valid. otherwise, we may crash late
        self.__process_gpu_cluster_recommendation(self.wrapper_options.get('gpuClusterRecommendation'))
        self.__process_filter_args(self.wrapper_options.get('filterApps'))

        self._process_offline_cluster_args()
        self._process_eventlogs_args()
        # This is noise to dump everything
        # self.logger.debug('%s custom arguments = %s', self.pretty_name(), self.ctxt.props['wrapperCtx'])

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
        # for backward compatibility, filter out non-existing columns
        existing_cols_subset = [col for col in cols_subset if col in all_rows.columns]
        cols_map = self.ctxt.get_value('toolOutput', 'csv', 'summaryReport', 'mapColumns')
        subset_data = all_rows.loc[:, existing_cols_subset]
        if cols_map:
            for col_rename in cols_map:
                subset_data.columns = subset_data.columns.str.replace(col_rename,
                                                                      cols_map.get(col_rename),
                                                                      regex=False)

        # for TCO, group by app name and average durations, then recalculate Estimated GPU Speedup
        group_map = self.ctxt.get_value('toolOutput', 'csv', 'summaryReport', 'groupColumns')
        if group_map:
            for group_key, group_value in group_map.items():
                subset_data[group_key] = subset_data.groupby(group_value)[group_key].transform('mean')

        drop_arr = self.ctxt.get_value('toolOutput', 'csv', 'summaryReport', 'dropDuplicates')
        subset_data = subset_data.drop_duplicates(subset=drop_arr)

        notes = []
        if len(subset_data) != len(all_rows):
            notes = 'Apps with the same name are grouped together and their metrics are averaged'

        subset_data['Estimated GPU Speedup'] = subset_data['App Duration'] / subset_data['Estimated GPU Duration']

        return subset_data, notes

    def __remap_cols_for_shape_type(self,
                                    data_set: pd.DataFrame,
                                    initial_cols_set: List[str],
                                    reshape_type: QualGpuClusterReshapeType) -> pd.DataFrame:
        cols_conf = self.ctxt.get_value('local', 'output', 'processDFProps',
                                        'clusterShapeCols', 'colsPerShapeType',
                                        QualGpuClusterReshapeType.tostring(reshape_type))
        deleted_cols = cols_conf.get('excludeColumns')
        cols_map = cols_conf.get('mapColumns')
        appended_cols = cols_conf.get('appendColumns')
        if deleted_cols:
            new_cols = [col for col in initial_cols_set if col not in deleted_cols]
        else:
            new_cols = initial_cols_set[:]
        if appended_cols:
            for col_conf in appended_cols:
                col_name = col_conf.get('columnName')
                col_ind = col_conf.get('index')
                if col_ind < 0 or col_ind >= len(new_cols):
                    new_cols.append(col_name)
                else:
                    new_cols.insert(col_ind, col_name)
        subset_data = data_set.loc[:, new_cols]
        if cols_map:
            for col_rename in cols_map:
                subset_data.columns = subset_data.columns.str.replace(col_rename,
                                                                      cols_map.get(col_rename),
                                                                      regex=False)

        return subset_data

    def __generate_mc_types_conversion_report(self):
        report_content = []
        if bool(self.ctxt.platform.ctxt['notes']):
            # get the converted instance types
            node_conversions = self.ctxt.platform.ctxt['notes'].get('nodeConversions')
            if node_conversions is not None:
                report_content = [
                    Utils.gen_report_sec_header('Instance types conversions', hrule=False),
                ]
                conversion_items = []
                for mc_src, mc_target in node_conversions.items():
                    conversion_items.append([mc_src, 'to', mc_target])
                report_content.append(tabulate(conversion_items))
                report_content.append(self.ctxt.platform.get_footer_message())
        return report_content

    def __generate_cluster_shape_report(self) -> str:
        if bool(self.ctxt.platform.ctxt['notes']):
            return Utils.gen_multiline_str(self.ctxt.platform.ctxt['notes'].get('clusterShape'))
        return None

    def __recommendation_is_non_standard(self):
        cluster_shape_type = self.ctxt.get_ctxt('gpuClusterShapeRecommendation')
        if cluster_shape_type:
            return cluster_shape_type != QualGpuClusterReshapeType.get_default()
        return False

    def __apply_non_standard_gpu_shape(self,
                                       all_apps: pd.DataFrame,
                                       cluster_workers_cnt: int,
                                       cluster_shape_t: QualGpuClusterReshapeType):
        min_w_cnt_from_conf = self.ctxt.platform.configs.get_value_silent('clusterSpecs',
                                                                          'minWorkerNodes')
        scale_factor_from_conf = self.ctxt.platform.configs.get_value_silent('clusterSpecs',
                                                                             'gpuScaleFactor')
        # get the min_worker_cnt from the qualification config in case it is not defined for the platform
        default_min_w_cnt = self.ctxt.get_value('local', 'output', 'processDFProps',
                                                'minimumWorkerCount')
        # get the scale factor from the qualification config in case it is not defined for the platform
        default_scale_factor = self.ctxt.get_value('local', 'output', 'processDFProps', 'gpuScaleFactor')
        # As you reduce nodes, performance will be slightly better than linear based on benchmarks
        scale_f = scale_factor_from_conf if scale_factor_from_conf else default_scale_factor
        min_w_cnt = min_w_cnt_from_conf if min_w_cnt_from_conf else default_min_w_cnt
        # calculate the reshape_cluster_column
        reshape_col = self.ctxt.get_value('local', 'output', 'processDFProps',
                                          'clusterShapeCols', 'columnName')
        speedup_col = 'Estimated GPU Speedup'
        gpu_dur_col = 'Estimated GPU Duration'
        cpu_dur_col = 'App Duration'

        def f_cell(x):
            return ceil(x * 100) / 100

        def calc_cluster_shape_col(df_row, min_worker_cnt: int, old_workers_cnt: int) -> pd.Series:
            gpu_speedup = df_row[speedup_col]
            # We should not worry about division by 0 because speedup is BGE 1.0
            cluster_shape = max(min_worker_cnt, ceil(scale_f * old_workers_cnt / gpu_speedup))
            return pd.Series([cluster_shape])

        def update_cols_with_new_shape(apps_df: pd.DataFrame,
                                       old_workers_cnt: int) -> (pd.DataFrame, bool):
            apps_df[gpu_dur_col] = apps_df.apply(lambda row: f_cell(
                (old_workers_cnt / row[reshape_col]) * scale_f * row[cpu_dur_col] / row[speedup_col]), axis=1)
            apps_df[speedup_col] = apps_df.apply(
                lambda row: f_cell(row[cpu_dur_col] / row[gpu_dur_col]), axis=1
            )
            return apps_df

        all_apps[[reshape_col]] = all_apps.apply(
            lambda row: calc_cluster_shape_col(row, min_w_cnt, cluster_workers_cnt), axis=1)
        recalc_speedups_flag = True
        if cluster_shape_t == QualGpuClusterReshapeType.CLUSTER:
            # the column value should be reset to the maximum of all the rows
            max_workers_cnt = all_apps[reshape_col].max()
            all_apps[reshape_col] = max_workers_cnt
            # Append a node to be part of the summary report
            reshape_msg_plain = self.ctxt.get_value('local', 'output', 'processDFProps',
                                                    'clusterShapeCols', 'noteMsg')
            self.ctxt.platform.update_ctxt_notes('clusterShape',
                                                 reshape_msg_plain.format(max_workers_cnt))
            # If max_workers_cnt EQ gpu_cluster nodes then no need to recalculate the columns
            recalc_speedups_flag = max_workers_cnt != cluster_workers_cnt
        # check if we need to recalculate the flags
        if not recalc_speedups_flag:
            return all_apps, False
        return update_cols_with_new_shape(all_apps, cluster_workers_cnt), True

    def __apply_gpu_cluster_reshape(self, all_apps: pd.DataFrame) -> (pd.DataFrame, bool):
        gpu_reshape_type = self.ctxt.get_ctxt('gpuClusterShapeRecommendation')
        gpu_cluster = ClusterReshape(self.ctxt.get_ctxt('gpuClusterProxy'))
        per_row_flag = False
        if self.__recommendation_is_non_standard():
            apps_df, per_row_flag = self.__apply_non_standard_gpu_shape(all_apps,
                                                                        gpu_cluster.get_workers_count(),
                                                                        gpu_reshape_type)
        else:
            apps_df = all_apps
        return apps_df, per_row_flag

    def __calc_apps_cost(self,
                         app_df_set: pd.DataFrame,
                         shape_col: str,
                         speedup_rec_col: str,
                         cost_per_row: bool = False):
        # used for the caching of the per-row estimator for optimizations
        saving_estimator_cache = {}
        savings_ranges = self.ctxt.get_value('local', 'output', 'processDFProps',
                                             'savingRecommendationsRanges')

        def get_costs_for_single_app(df_row, estimator: SavingsEstimator) -> pd.Series:
            cpu_cost, gpu_cost, est_savings = estimator.get_costs_and_savings(df_row['App Duration'],
                                                                              df_row['Estimated GPU Duration'])
            # We do not want to mistakenly mark a Not-applicable app as Recommended in the savings column
            if df_row[speedup_rec_col] == 'Not Applicable':
                savings_recommendations = 'Not Applicable'
            else:
                for s_range in savings_ranges.values():
                    if s_range.get('lowerBound') <= est_savings < s_range.get('upperBound'):
                        savings_recommendations = s_range.get('title')
                        break

            # For TCO, calculating annual cost savings based on job frequency
            job_frequency = 30  # default frequency is daily
            if 'Estimated Job Frequency (monthly)' in df_row:
                job_frequency = df_row['Estimated Job Frequency (monthly)']
            annual_cost_savings = job_frequency * 12 * (cpu_cost - gpu_cost)

            return pd.Series([savings_recommendations, cpu_cost, gpu_cost,
                              est_savings, job_frequency, annual_cost_savings])

        def get_cost_per_row(df_row, reshape_col: str) -> pd.Series:
            nonlocal saving_estimator_cache
            workers_cnt = df_row[reshape_col]
            estimator_obj = saving_estimator_cache.get(workers_cnt)
            if not estimator_obj:
                # create the object and add it to the caching dict
                reshaped_cluster = ClusterReshape(self.ctxt.get_ctxt('gpuClusterProxy'),
                                                  reshape_workers_cnt=lambda x: workers_cnt)
                estimator_obj = self.ctxt.platform.create_saving_estimator(self.ctxt.get_ctxt('cpuClusterProxy'),
                                                                           reshaped_cluster)
                saving_estimator_cache.setdefault(workers_cnt, estimator_obj)
            cost_pd_series = get_costs_for_single_app(df_row, estimator_obj)
            return cost_pd_series

        cost_cols = self.ctxt.get_value('local', 'output', 'costColumns')
        if not cost_per_row:
            # initialize the savings estimator only once
            reshaped_gpu_cluster = ClusterReshape(self.ctxt.get_ctxt('gpuClusterProxy'))
            savings_estimator = self.ctxt.platform.create_saving_estimator(self.ctxt.get_ctxt('cpuClusterProxy'),
                                                                           reshaped_gpu_cluster)
            app_df_set[cost_cols] = app_df_set.apply(
                lambda row: get_costs_for_single_app(row, estimator=savings_estimator), axis=1)
        else:
            # this is per row calculation and saving estimator should be created for each row
            app_df_set[cost_cols] = app_df_set.apply(
                lambda row: get_cost_per_row(row, shape_col), axis=1)
        return app_df_set

    def __build_global_report_summary(self,
                                      all_apps: pd.DataFrame,
                                      csv_out: str) -> QualificationSummary:
        if all_apps.empty:
            # No need to run saving estimator or process the data frames.
            return QualificationSummary(comments=self.__generate_mc_types_conversion_report())

        apps_pruned_df, prune_notes = self.__remap_columns_and_prune(all_apps)
        recommended_apps = self.__get_recommended_apps(apps_pruned_df)
        # if the gpu_reshape_type is set to JOB then, then we should ignore recommended apps
        speedups_irrelevant_flag = self.__recommendation_is_non_standard()
        reshaped_notes = self.__generate_cluster_shape_report()
        report_comments = [prune_notes] if prune_notes else []
        if reshaped_notes:
            report_comments.append(reshaped_notes)

        pricing_config = self.ctxt.platform.configs.get_value_silent('pricing')
        target_platform = self.ctxt.get_ctxt('targetPlatform')
        if target_platform is not None:
            pricing_config = self.ctxt.platform.configs.get_value_silent('csp_pricing')
        reshape_col = self.ctxt.get_value('local', 'output', 'processDFProps',
                                          'clusterShapeCols', 'columnName')
        speed_recommendation_col = self.ctxt.get_value('local', 'output', 'speedupRecommendColumn')
        apps_reshaped_df, per_row_flag = self.__apply_gpu_cluster_reshape(apps_pruned_df)
        # OnPrem platform doesn't have pricing information. We do not calculate cost savings for
        # OnPrem platform if the target_platform is not specified.
        if pricing_config is None:
            df_final_result = apps_reshaped_df
            if not apps_reshaped_df.empty:
                # Do not include estimated job frequency in csv file
                apps_reshaped_df = apps_reshaped_df.drop(columns=['Estimated Job Frequency (monthly)'])
                self.logger.info('Generating GPU Estimated Speedup as %s', csv_out)
                apps_reshaped_df.to_csv(csv_out, float_format='%.2f')
        else:
            # Now, the dataframe is ready to calculate the cost and the savings
            apps_working_set = self.__calc_apps_cost(apps_reshaped_df,
                                                     reshape_col,
                                                     speed_recommendation_col,
                                                     per_row_flag)
            df_final_result = apps_working_set
            if not apps_working_set.empty:
                self.logger.info('Generating GPU Estimated Speedup and Savings as %s', csv_out)
                # we can use the general format as well but this will transform numbers to E+. So, stick with %f
                apps_working_set.to_csv(csv_out, float_format='%.2f')

        return QualificationSummary(comments=report_comments,
                                    all_apps=apps_pruned_df,
                                    recommended_apps=recommended_apps,
                                    pricing_config=pricing_config,
                                    df_result=df_final_result,
                                    irrelevant_speedups=speedups_irrelevant_flag,
                                    sections_generators=[self.__generate_mc_types_conversion_report])

    def _process_output(self):
        def process_df_for_stdout(raw_df):
            """
            process the dataframe to be more readable on the stdout
            1- convert time durations to second
            2- shorten headers
            """
            selected_cols = self.ctxt.get_value('local', 'output', 'summaryColumns')
            # check if any filters apply
            filter_recommendation_enabled = self.ctxt.get_ctxt('filterApps') == QualFilterApp.SPEEDUPS
            filter_pos_enabled = self.ctxt.get_ctxt('filterApps') == QualFilterApp.SAVINGS
            if self.__recommendation_is_non_standard():
                # During processing of arguments phase, we verified that the filter does not conflict
                # with the shape recommendation
                raw_df = self.__remap_cols_for_shape_type(raw_df,
                                                          selected_cols,
                                                          self.ctxt.get_ctxt('gpuClusterShapeRecommendation'))
                # update the selected columns
                selected_cols = list(raw_df.columns)

            pricing_config = self.ctxt.platform.configs.get_value_silent('pricing')
            target_platform = self.ctxt.get_ctxt('targetPlatform')
            if pricing_config is None and target_platform is None:
                selected_cols = list(raw_df.columns)
            # filter by recommendations if enabled
            if filter_recommendation_enabled:
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
                    self.ctxt.set_ctxt('wrapperOutputContent',
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
                col_w_conf = self.ctxt.get_value('toolOutput', 'stdout', 'summaryReport', 'columnWidth')
                for column in df_row.columns:
                    if len(column) > col_w_conf:
                        new_column_name = textwrap.fill(column, col_w_conf, break_long_words=False)
                        if new_column_name != column:
                            df_row.columns = df_row.columns.str.replace(column,
                                                                        new_column_name, regex=False)
            return df_row

        if not self._evaluate_rapids_jar_tool_output_exist():
            return
        rapids_output_dir = self.ctxt.get_rapids_output_folder()
        rapids_summary_file = FSUtil.build_path(rapids_output_dir,
                                                self.ctxt.get_value('toolOutput', 'csv', 'summaryReport', 'fileName'))
        self.ctxt.logger.debug('Rapids CSV summary file is located as: %s', rapids_summary_file)
        df = pd.read_csv(rapids_summary_file)
        csv_file_name = self.ctxt.get_value('local', 'output', 'fileName')
        csv_summary_file = FSUtil.build_path(self.ctxt.get_output_folder(), csv_file_name)
        report_gen = self.__build_global_report_summary(df, csv_summary_file)
        summary_report = report_gen.generate_report(app_name=self.pretty_name(),
                                                    wrapper_csv_file=csv_summary_file,
                                                    csp_report_provider=self._generate_platform_report_sections,
                                                    df_pprinter=process_df_for_stdout,
                                                    output_pprinter=self._report_tool_full_location)
        self.ctxt.set_ctxt('wrapperOutputContent', summary_report)

    def _write_summary(self):
        wrapper_out_content = self.ctxt.get_ctxt('wrapperOutputContent')
        if wrapper_out_content is not None:
            print(Utils.gen_multiline_str(wrapper_out_content))

    def _init_rapids_arg_list(self) -> List[str]:
        # TODO: Make sure we add this argument only for jar versions 23.02+
        return ['--platform', self.ctxt.platform.get_platform_name().replace('_', '-')]

    def _generate_section_lines(self, sec_conf: dict) -> List[str]:
        if sec_conf.get('sectionID') == 'initializationScript':
            # format the initialization scripts
            reshaped_gpu_cluster = ClusterReshape(self.ctxt.get_ctxt('gpuClusterProxy'))
            gpu_per_machine, gpu_device = reshaped_gpu_cluster.get_gpu_per_worker()
            fill_map = {
                0: self.ctxt.platform.cli.get_region(),
                1: [gpu_device.lower(), gpu_per_machine]
            }
            res = []
            # TODO: improve the display of code snippets by using module pygments (for bash)
            #   module code can be used for python snippets
            for ind, l_str in enumerate(sec_conf['content'].get('lines')):
                if ind in fill_map:
                    rep_var = fill_map.get(ind)
                    new_value = l_str.format(*rep_var) if isinstance(rep_var, list) else l_str.format(rep_var)
                    res.append(new_value)
                else:
                    res.append(l_str)
            return res
        if sec_conf.get('sectionID') == 'gpuClusterCreationScript':
            gpu_cluster = self.ctxt.get_ctxt('gpuClusterProxy')
            script_content = gpu_cluster.generate_create_script()
            highlighted_code = TemplateGenerator.highlight_bash_code(script_content)
            return ['```bash', highlighted_code, '```']
        if sec_conf.get('sectionID') == 'runUserToolsBootstrap':
            gpu_cluster = self.ctxt.get_ctxt('gpuClusterProxy')
            override_args = {'CLUSTER_NAME': '$CLUSTER_NAME'}
            script_content = gpu_cluster.generate_bootstrap_script(overridden_args=override_args)
            highlighted_code = TemplateGenerator.highlight_bash_code(script_content)
            return ['```bash', highlighted_code, '```', '']
        return super()._generate_section_content(sec_conf)


@dataclass
class QualificationAsLocal(Qualification):
    """
    Qualification tool running on local development.
    """
    description: str = 'This is the localQualification'

    def _copy_dependencies_to_remote(self):
        self.logger.info('Skipping preparing remote dependency folder')

    def _process_job_submission_args(self):
        self._process_local_job_submission_args()

    def _prepare_job_arguments(self):
        self._prepare_local_job_arguments()

    def _delete_remote_dep_folder(self):
        self.logger.debug('Local mode skipping deleting the remote workdir')

    def _download_remote_output_folder(self):
        self.logger.debug('Local mode skipping downloading the remote output workdir')

    def _archive_results(self):
        self._archive_local_results()
