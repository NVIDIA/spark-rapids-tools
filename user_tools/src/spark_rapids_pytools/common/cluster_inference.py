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

"""This module provides functionality for cluster inference"""

from dataclasses import dataclass, field

from typing import Optional
from logging import Logger

import pandas as pd

from spark_rapids_pytools.cloud_api.sp_types import PlatformBase, ClusterBase
from spark_rapids_pytools.common.prop_manager import JSONPropertiesContainer
from spark_rapids_pytools.common.utilities import ToolLogging


@dataclass
class ClusterInference:
    """
    Class for inferring cluster information and constructing CPU clusters.

    :param platform: The platform on which the cluster inference is performed.
    """
    platform: PlatformBase = field(default=None, init=True)
    logger: Logger = field(default=ToolLogging.get_and_setup_logger('rapids.tools.cluster_inference'), init=False)

    def get_cluster_template_args(self, cluster_info_df: pd.Series) -> Optional[dict]:
        """
        Extract information about drivers and executors from input json
        """
        # Currently we support only single driver node for all CSPs
        num_driver_nodes = 1
        driver_instance = cluster_info_df.get('Driver Instance')
        # If driver instance is not set, use the default value from platform configurations
        if pd.isna(driver_instance):
            driver_instance = self.platform.configs.get_value('clusterInference', 'defaultCpuInstances', 'driver')
        num_executor_nodes = cluster_info_df.get('Num Executor Nodes')
        executor_instance = cluster_info_df.get('Executor Instance')
        if pd.isna(executor_instance):
            # If executor instance is not set, use the default value based on the number of cores
            cores_per_executor = cluster_info_df.get('Cores Per Executor')
            executor_instance = self.platform.get_matching_executor_instance(cores_per_executor)
            if pd.isna(executor_instance):
                self.logger.info('Unable to infer CPU cluster. No matching executor instance found for vCPUs = %s',
                                 cores_per_executor)
                return None
        return {
            'DRIVER_INSTANCE': f'"{driver_instance}"',
            'NUM_DRIVER_NODES': num_driver_nodes,
            'EXECUTOR_INSTANCE': f'"{executor_instance}"',
            'NUM_EXECUTOR_NODES': num_executor_nodes
        }

    def infer_cpu_cluster(self, cluster_info_df: pd.DataFrame) -> Optional[ClusterBase]:
        """
        Infer CPU cluster configuration based on json input and return the constructed cluster object.
        """
        if len(cluster_info_df) != 1:
            self.logger.info('Cannot infer CPU cluster from event logs. Only single cluster is supported.')
            return None

        # Extract cluster information from parsed logs. Above check ensures df contains single row.
        cluster_template_args = self.get_cluster_template_args(cluster_info_df.iloc[0])
        if cluster_template_args is None:
            return None
        # Construct cluster configuration using platform-specific logic
        cluster_conf = self.platform.generate_cluster_configuration(cluster_template_args)
        if cluster_conf is None:
            return None
        cluster_props_new = JSONPropertiesContainer(cluster_conf, file_load=False)
        return self.platform.load_cluster_by_prop(cluster_props_new, is_inferred=True)
