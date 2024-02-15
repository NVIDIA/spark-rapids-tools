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

    def get_cluster_info(self, cluster_info_json: JSONPropertiesContainer) -> dict:
        """
        Extract information about drivers and executors from input json
        """
        num_executor_nodes = cluster_info_json.get_value_silent('numExecutorNodes')
        cores_per_executor = cluster_info_json.get_value_silent('coresPerExecutor')
        driver_instance = cluster_info_json.get_value_silent('driverInstance')
        executor_instance = cluster_info_json.get_value_silent('executorInstance')
        # If driver instance is not set, use the default value from platform configurations
        if driver_instance is None:
            driver_instance = self.platform.configs.get_value('clusterInference', 'defaultCpuInstances', 'driver')
        # If executor instance is not set, use the default value based on the number of cores
        if executor_instance is None:
            default_instances = self.platform.configs.get_value('clusterInference', 'defaultCpuInstances', 'executor')
            matching_instance = next(
                (instance['name'] for instance in default_instances if instance['vCPUs'] == cores_per_executor),
                None
            )
            if matching_instance is None:
                self.logger.info(
                    f'Unable to infer CPU cluster. No matching executor instance found for vCPUs = {cores_per_executor}')
                return None
            executor_instance = matching_instance
        return {
            'driver_instance': driver_instance,
            'num_executor_nodes': num_executor_nodes,
            'executor_instance': executor_instance
        }

    def infer_cpu_cluster(self, cluster_info_json: JSONPropertiesContainer) -> Optional[ClusterBase]:
        """
        Infer CPU cluster configuration based on json input and return the constructed cluster object.
        """
        # Extract cluster information from parsed logs
        cluster_info = self.get_cluster_info(cluster_info_json)
        if cluster_info is None:
            return None
        # Construct cluster configuration using platform-specific logic
        cluster_conf = self.platform.construct_cluster_config(cluster_info)
        cluster_props = JSONPropertiesContainer(cluster_conf, file_load=False)
        return self.platform.load_cluster_by_prop(cluster_props, is_inferred=True)
