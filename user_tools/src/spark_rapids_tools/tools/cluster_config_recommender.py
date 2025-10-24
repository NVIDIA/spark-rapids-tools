# Copyright (c) 2024-2025, NVIDIA CORPORATION.
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
from typing import Optional, Dict, Union
from logging import Logger

import pandas as pd

from spark_rapids_pytools.cloud_api.sp_types import ClusterBase
from spark_rapids_pytools.rapids.tool_ctxt import ToolContext
from spark_rapids_pytools.common.utilities import ToolLogging
from spark_rapids_tools import CspEnv


@dataclass
class ClusterRecommendationInfo:
    """
    Dataclass to hold the recommended cluster and the qualified cluster recommendation.
    """
    platform: str = field(default=CspEnv.get_default(), init=True)
    source_cluster_config: dict = field(default_factory=dict)
    recommended_cluster_config: dict = field(default_factory=dict)
    qualified_cluster_recommendation: str = 'Not Available'

    def _cluster_info_to_dict(self) -> Dict[str, dict]:
        """
        Returns the cluster info as a dictionary. Since this will be a value of a column, the keys are in
        camelCase to match the JSON format.
        """
        return {
            'platform': self.platform,
            'sourceCluster': self.source_cluster_config,
            'recommendedCluster': self.recommended_cluster_config
        }

    def to_dict(self) -> Dict[str, Union[dict, str]]:
        return {
            'Cluster Info': self._cluster_info_to_dict(),
            'Qualified Cluster Recommendation': self.qualified_cluster_recommendation
        }

    @classmethod
    def get_default_dict(cls) -> Dict[str, Union[dict, str]]:
        return cls().to_dict()


@dataclass
class TuningRecommendationInfo:
    """
    Dataclass to hold the full cluster config recommendations and the GPU config recommendation
    """
    cluster_config_recommendations: pd.Series
    gpu_config_recommendation: pd.Series

    def to_dict(self) -> Dict[str, pd.Series]:
        return {
            'Full Cluster Config Recommendations*': self.cluster_config_recommendations,
            'GPU Config Recommendation Breakdown*': self.gpu_config_recommendation,
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
          'Cluster Info': {
             'sourceCluster': {'driverNodeType': 'm6.xlarge', 'workerNodeType': 'm6.xlarge', 'numWorkerNodes': 2 }
             'recommendedCluster': {'driverNodeType': 'm6.xlarge', 'workerNodeType': 'g5.2xlarge', 'numWorkerNodes': 2 }
           },
          'Qualified Cluster Recommendation': '2 x g5.2xlarge'
        }
        """  # pylint: disable=line-too-long
        # Return None if no GPU cluster is available.
        # If no CPU cluster is available, we can still recommend based on the inferred GPU cluster in the Scala tool.
        if not gpu_cluster:
            return None

        platform = gpu_cluster.platform.get_platform_name()
        cpu_cluster_config = cpu_cluster.get_cluster_configuration() if cpu_cluster else {}
        gpu_cluster_config = gpu_cluster.get_cluster_configuration()
        gpu_conversion_str = gpu_cluster.get_worker_conversion_str()

        return ClusterRecommendationInfo(platform, cpu_cluster_config, gpu_cluster_config, gpu_conversion_str)

    def _get_cluster_conversion_summary(self) -> Dict[str, ClusterRecommendationInfo]:
        """
        Generates a summary of the cluster conversions from CPU to GPU instance types.
        Returns a dictionary with either:
        i. 'all' -> `ClusterRecommendationInfo`  for all instances
        ii. '<app_id>' -> `ClusterRecommendationInfo`  for each app
        """
        cluster_conversion_summary = {}
        cpu_cluster_info_per_app = self.ctxt.get_ctxt('cpuClusterInfoPerApp')
        gpu_cluster_info_per_app = self.ctxt.get_ctxt('gpuClusterInfoPerApp')
        if cpu_cluster_info_per_app and gpu_cluster_info_per_app:
            for app_id in cpu_cluster_info_per_app:
                cpu_info = cpu_cluster_info_per_app.get(app_id)
                gpu_info = gpu_cluster_info_per_app.get(app_id)
                conversion_summary = self._get_instance_type_conversion(cpu_info, gpu_info)
                if conversion_summary:
                    cluster_conversion_summary[app_id] = conversion_summary

        return cluster_conversion_summary

    def _get_tuning_summary(self, tools_processed_apps: pd.DataFrame) -> TuningRecommendationInfo:
        """
        Get the tuning recommendations for the processed apps using the new tuning API.
        Returns paths with app context (e.g., tuning_apps/<app-id>/combined.conf).
        """
        missing_msg = 'Does not exist, see log for errors'

        # Get the core handler from context to access tuning API
        core_handler = self.ctxt.get_ctxt('coreHandler')
        if 'App ID' not in tools_processed_apps.columns or core_handler is None:
            if core_handler is None:
                self.logger.warning('QualCoreHandler not found in context, cannot access tuning files')
            return TuningRecommendationInfo.get_default(missing_msg, tools_processed_apps.shape[0])

        # Initialize series with paths that include app context
        app_ids = tools_processed_apps['App ID']
        combined_conf_paths = pd.Series([missing_msg] * len(app_ids), index=app_ids.index)
        recommendations_log_paths = pd.Series([missing_msg] * len(app_ids), index=app_ids.index)

        # For each app, check if tuning files exist using the API
        for index, app_id in app_ids.items():
            try:
                # Check if recommendations.log exists in the new tuning_apps structure
                recommendations_result = core_handler.txt('tuningRecommendationsLog').app(app_id).load()
                if recommendations_result.success:
                    # Files exist, return paths with app context
                    combined_conf_paths.at[index] = f'tuning_apps/{app_id}/combined.conf'
                    recommendations_log_paths.at[index] = f'tuning_apps/{app_id}/recommendations.log'

            except Exception as e:  # pylint: disable=broad-except
                self.logger.debug('Could not load tuning files for app %s: %s', app_id, e)

        return TuningRecommendationInfo(combined_conf_paths, recommendations_log_paths)

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
            if len(cluster_conversion_summary) > 0:
                # Add the per-app node conversions
                conversion_summary_flattened = {k: v.to_dict() for k, v in cluster_conversion_summary.items()}
                conversion_df = pd.DataFrame.from_dict(conversion_summary_flattened, orient='index').reset_index()
                conversion_df.rename(columns={'index': 'App ID'}, inplace=True)
                result_df = pd.merge(tools_processed_apps, conversion_df, on=['App ID'], how='left')

            # 1b. Fill in the missing values of the cluster conversion columns with default values
            for col, default_val in ClusterRecommendationInfo.get_default_dict().items():  # type: str, Union[dict, str]
                if col not in result_df:
                    result_df[col] = [default_val] * result_df.shape[0]
                else:
                    def fill_na_fn(x, def_val):
                        return def_val if pd.isna(x) else x
                    # Using partial to avoid closure issues with lambda in apply()
                    fill_na_fn_partial = partial(fill_na_fn, def_val=default_val)
                    result_df[col] = result_df[col].apply(fill_na_fn_partial)

            # 2. Add tuning configuration recommendations per app to the processed apps
            tuning_recommendation_summary = self._get_tuning_summary(result_df)
            for col, tuning_summaries in tuning_recommendation_summary.to_dict().items():  # type: str, pd.Series
                result_df[col] = tuning_summaries
            return result_df
        except Exception as e:  # pylint: disable=broad-except
            self.logger.warning('Error while adding cluster and tuning recommendations. Reason - %s:%s',
                                type(e).__name__, e)
            return tools_processed_apps
