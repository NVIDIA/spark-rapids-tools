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

""" This module provides functionality for cluster shape recommendation """

from functools import partial
from dataclasses import dataclass, field
from typing import Optional
from logging import Logger

import pandas as pd

from spark_rapids_pytools.cloud_api.sp_types import ClusterBase
from spark_rapids_pytools.common.sys_storage import FSUtil
from spark_rapids_pytools.rapids.tool_ctxt import ToolContext
from spark_rapids_pytools.common.utilities import ToolLogging


@dataclass
class ClusterRecommendationInfo:
    """
    Dataclass to hold the recommended cluster and the qualified node recommendation.
    """
    source_cluster_config: dict = field(default_factory=dict)
    recommended_cluster_config: dict = field(default_factory=dict)
    qualified_node_recommendation: str = 'Not Available'

    def to_dict(self) -> dict:
        return {
            'Source Cluster': self.source_cluster_config,
            'Recommended Cluster': self.recommended_cluster_config,
            'Qualified Node Recommendation': self.qualified_node_recommendation
        }

    @classmethod
    def get_default_dict(cls) -> dict:
        return cls().to_dict()


@dataclass
class TuningRecommendationInfo:
    """
    Dataclass to hold the full cluster config recommendations and the GPU config recommendation
    """
    cluster_config_recommendations: pd.Series
    gpu_config_recommendation: pd.Series

    def to_dict(self) -> dict:
        return {
            'Full Cluster Config Recommendations*': self.cluster_config_recommendations.to_list(),
            'GPU Config Recommendation Breakdown*': self.gpu_config_recommendation.to_list()
        }

    @classmethod
    def get_default(cls, missing_msg: str, num_apps: int = 0) -> 'TuningRecommendationInfo':
        default_series = pd.Series([missing_msg] * num_apps)
        return cls(cluster_config_recommendations=default_series, gpu_config_recommendation=default_series)


@dataclass
class ClusterConfigRecommender:
    """
    Class for recommending cluster shape and tuning configurations for processed apps.
    """
    ctxt: ToolContext = field(default=None, init=True)
    logger: Logger = field(default=ToolLogging.get_and_setup_logger('rapids.tools.cluster_recommender'), init=False)

    @classmethod
    def _get_instance_type_conversion(cls, cpu_cluster: Optional[ClusterBase],
                                      gpu_cluster: Optional[ClusterBase]) -> Optional[ClusterRecommendationInfo]:
        """
        Helper method to determine the conversion summary between CPU and GPU instance types.
        Generate the cluster shape recommendation as:
        {
          'Source Cluster': {'driverInstance': 'm6.xlarge', 'executorInstance': 'm6.xlarge', 'numExecutorNodes': 2 }
          'Recommended Cluster': {'driverInstance': 'm6.xlarge', 'executorInstance': 'g5.2xlarge', 'numExecutorNodes': 2 }
          'Qualified Node Recommendation': 'm6.xlarge to g5.2xlarge'
        }
        """  # pylint: disable=line-too-long
        # Return None if no GPU cluster is available.
        # If no CPU cluster is available, we can still recommend based on the inferred GPU cluster in the Scala tool.
        if not gpu_cluster:
            return None

        gpu_instance_type = gpu_cluster.get_worker_node().instance_type
        recommended_cluster_config = gpu_cluster.get_cluster_configuration()
        conversion_str = gpu_instance_type
        source_cluster_config = {}

        if cpu_cluster:
            source_cluster_config = cpu_cluster.get_cluster_configuration()
            cpu_instance_type = cpu_cluster.get_worker_node().instance_type
            if cpu_instance_type != gpu_instance_type:
                conversion_str = f'{cpu_instance_type} to {gpu_instance_type}'
        return ClusterRecommendationInfo(source_cluster_config, recommended_cluster_config, conversion_str)

    def _get_cluster_conversion_summary(self) -> dict:
        """
        Generates a summary of the cluster conversions from CPU to GPU instance types.
        Returns a dictionary with either:
        i. 'all' -> `ClusterRecommendationInfo`  for all instances
        ii. '<app_id>' -> `ClusterRecommendationInfo`  for each app
        """
        cluster_conversion_summary = {}
        # Summary for all instances
        cpu_cluster_info = self.ctxt.get_ctxt('cpuClusterProxy')
        gpu_cluster_info = self.ctxt.get_ctxt('gpuClusterProxy')
        conversion_summary_all = self._get_instance_type_conversion(cpu_cluster_info, gpu_cluster_info)
        if conversion_summary_all:
            self._log_cluster_conversion(cpu_cluster_info, gpu_cluster_info)
            cluster_conversion_summary['all'] = conversion_summary_all.to_dict()

        # Summary for each app
        cpu_cluster_info_per_app = self.ctxt.get_ctxt('cpuClusterInfoPerApp')
        gpu_cluster_info_per_app = self.ctxt.get_ctxt('gpuClusterInfoPerApp')
        if cpu_cluster_info_per_app and gpu_cluster_info_per_app:
            for app_id in cpu_cluster_info_per_app:
                cpu_info = cpu_cluster_info_per_app.get(app_id)
                gpu_info = gpu_cluster_info_per_app.get(app_id)
                conversion_summary = self._get_instance_type_conversion(cpu_info, gpu_info)
                if conversion_summary:
                    self._log_cluster_conversion(cpu_info, gpu_info, app_id)
                    cluster_conversion_summary[app_id] = conversion_summary.to_dict()

        return cluster_conversion_summary

    def _get_tuning_summary(self, tools_processed_apps: pd.DataFrame) -> TuningRecommendationInfo:
        """
        Get the tuning recommendations for the processed apps.
        """
        rapids_output_dir = self.ctxt.get_rapids_output_folder()
        auto_tuning_path = FSUtil.build_path(rapids_output_dir,
                                             self.ctxt.get_value('toolOutput', 'csv', 'tunings', 'subFolder'))
        missing_msg = 'Does not exist, see log for errors'
        if 'App ID' not in tools_processed_apps.columns:
            return TuningRecommendationInfo.get_default(missing_msg, tools_processed_apps.shape[0])

        full_tunings_file: pd.Series = tools_processed_apps['App ID'] + '.conf'
        gpu_tunings_file: pd.Series = tools_processed_apps['App ID'] + '.log'
        # check to see if the tuning are actually there, assume if one tuning file is there,
        # the other will be as well.
        tunings_abs_path = FSUtil.get_abs_path(auto_tuning_path)
        if FSUtil.resource_exists(tunings_abs_path):  # check if the file exists
            for index, file in gpu_tunings_file.items():
                full_tunings_path = auto_tuning_path + '/' + file
                abs_path = FSUtil.get_abs_path(full_tunings_path)
                if not FSUtil.resource_exists(abs_path):  # check if the file exists
                    gpu_tunings_file.at[index] = missing_msg
                    full_tunings_file.at[index] = missing_msg
            return TuningRecommendationInfo(full_tunings_file, gpu_tunings_file)
        return TuningRecommendationInfo.get_default(missing_msg, tools_processed_apps.shape[0])

    def add_cluster_and_tuning_recommendations(self, tools_processed_apps: pd.DataFrame) -> pd.DataFrame:
        """
        Adds columns for cluster configuration recommendations and tuning configurations to the processed apps.
        """
        if tools_processed_apps.empty:
            return tools_processed_apps

        try:
            result_df = tools_processed_apps.copy()
            # 1a. Add cluster conversion recommendations to apps
            cluster_conversion_summary = self._get_cluster_conversion_summary()
            # 'all' is a special indication that all the applications need to use this same node
            # recommendation vs the recommendations being per application
            if 'all' in cluster_conversion_summary:
                # Add cluster conversion columns to all apps
                for col, val in cluster_conversion_summary['all'].items():
                    result_df[col] = [val] * result_df.shape[0]
            elif len(cluster_conversion_summary) > 0:
                # Add the per-app node conversions
                conversion_df = pd.DataFrame.from_dict(cluster_conversion_summary, orient='index').reset_index()
                conversion_df.rename(columns={'index': 'App ID'}, inplace=True)
                result_df = pd.merge(tools_processed_apps, conversion_df, on=['App ID'], how='left')

            # 1b. Fill in the missing values of the cluster conversion columns with default values
            for col, replacement in ClusterRecommendationInfo.get_default_dict().items():
                # Using partial to avoid closure issues with lambda in apply()
                fill_na_fn = partial(lambda x, def_val: def_val if pd.isna(x) else x, def_val=replacement)
                col_dtype = 'str' if isinstance(replacement, str) else 'object'
                if col not in result_df:
                    # Add the column if it doesn't exist and fill it with NA
                    result_df[col] = pd.Series([pd.NA] * len(result_df), dtype=col_dtype)
                result_df[col] = result_df[col].apply(fill_na_fn)

            # 2. Add tuning configuration recommendations to all apps
            tuning_recommendation_summary = self._get_tuning_summary(result_df)
            for col, val in tuning_recommendation_summary.to_dict().items():
                result_df[col] = val
            return result_df
        except Exception as e:  # pylint: disable=broad-except
            self.logger.warning('Error while adding cluster and tuning recommendations. Reason - %s:%s',
                                type(e).__name__, e)
            return tools_processed_apps

    def _log_cluster_conversion(self, cpu_cluster: Optional[ClusterBase], gpu_cluster: Optional[ClusterBase],
                                app_id: Optional[str] = None) -> None:
        """
        Log the cluster conversion summary
        """
        cpu_cluster_str = cpu_cluster.get_cluster_shape_str() if cpu_cluster else 'N/A'
        gpu_cluster_str = gpu_cluster.get_cluster_shape_str() if gpu_cluster else 'N/A'
        conversion_log_msg = f'CPU cluster: {cpu_cluster_str}; Recommended GPU cluster: {gpu_cluster_str}'
        if cpu_cluster and cpu_cluster.is_inferred:  # If cluster is inferred, add it to the log message
            conversion_log_msg = f'Inferred {conversion_log_msg}'
        if app_id:  # If app_id is provided, add it to the log message
            conversion_log_msg = f'For App ID: {app_id}, {conversion_log_msg}'
        self.logger.info(conversion_log_msg)
