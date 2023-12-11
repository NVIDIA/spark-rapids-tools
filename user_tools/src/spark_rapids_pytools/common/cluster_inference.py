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

"""This module provides functionality for cluster inference using Spark event logs"""

from dataclasses import dataclass, field

from pathlib import Path
from logging import Logger

from spark_rapids_pytools.cloud_api.databricks_aws import DBAWSPlatform
from spark_rapids_pytools.cloud_api.databricks_azure import DBAzurePlatform
from spark_rapids_pytools.cloud_api.sp_types import PlatformBase
from spark_rapids_pytools.common.prop_manager import JSONPropertiesContainer
from spark_rapids_pytools.common.utilities import ToolLogging


@dataclass
class ClusterInference:
    """
    Class for inferring cluster information from Spark event logs and constructing CPU clusters.

    :param platform: The platform on which the cluster inference is performed.
    """
    platform: PlatformBase = field(default=None, init=True)
    logger: Logger = field(default=ToolLogging.get_and_setup_logger('rapids.tools.cluster_inference'), init=False)

    @staticmethod
    def _load_event_logs(eventlog_arg) -> list:
        """
        Read the event log file and return a list of event log properties containers.
        """
        eventlog_path = Path(eventlog_arg)
        if not eventlog_path.exists():
            raise FileNotFoundError(f"Path '{eventlog_path}' does not exist.")
        with eventlog_path.open(encoding='utf-8') as file:
            event_logs = [JSONPropertiesContainer(line, file_load=False) for line in file]
        return event_logs

    def get_cluster_info(self, eventlog_props: list):
        """
        Process event logs and extract information about drivers and executors.
        """
        num_driver_nodes = 0
        hosts = set()
        num_cores = None
        driver_instance = None
        executor_instance = None
        is_databricks = isinstance(self.platform, (DBAWSPlatform, DBAzurePlatform))

        for event_prop in eventlog_props:
            event_type = event_prop.get_value_silent('Event')

            # Check for Databricks environment update event to get driver and executor instances
            if is_databricks and driver_instance is None and event_type == 'SparkListenerEnvironmentUpdate':
                driver_instance = event_prop.get_value('Spark Properties', 'spark.databricks.driverNodeTypeId')
                executor_instance = event_prop.get_value('Spark Properties', 'spark.databricks.workerNodeTypeId')

            # Check for executor added event to get the number of cores
            if num_cores is None and event_type == 'SparkListenerExecutorAdded':
                num_cores = event_prop.get_value('Executor Info', 'Total Cores')

            # Check for BlockManager added event to count drivers and collect unique hosts
            elif event_type == 'SparkListenerBlockManagerAdded':
                executor_id = event_prop.get_value('Block Manager ID', 'Executor ID')
                if executor_id == 'driver':
                    num_driver_nodes += 1
                else:
                    host = event_prop.get_value('Block Manager ID', 'Host')
                    hosts.add(host)

        # If driver instance is not set, use the default value from platform configurations
        if driver_instance is None:
            driver_instance = self.platform.configs.get_value('clusterInference', 'defaultCpuInstances', 'driver')

        # If executor instance is not set, use the default value based on the number of cores
        if executor_instance is None:
            default_instances = self.platform.configs.get_value('clusterInference', 'defaultCpuInstances', 'executor')
            matching_instance = next(
                (instance['name'] for instance in default_instances if instance['vCPUs'] == num_cores),
                None
            )
            if matching_instance is None:
                self.logger.info(f'No matching executor instance found for vCPUs = {num_cores}')
                return None
            executor_instance = matching_instance
        return {
            'num_driver_nodes': num_driver_nodes,
            'driver_instance': driver_instance,
            'num_executor_nodes': len(hosts),  # Number of unique hosts identify number of executor nodes
            'executor_instance': executor_instance
        }

    def infer_cpu_cluster(self, eventlog_arg):
        """
        Infer CPU cluster configuration based on event logs and return the constructed cluster object.
        """
        parsed_log = self._load_event_logs(eventlog_arg)
        if len(parsed_log) == 0:
            return None

        cluster_info = self.get_cluster_info(parsed_log)
        if cluster_info is None:
            return None
        cluster_conf = self.platform.construct_cluster_config(cluster_info)
        cluster_props = JSONPropertiesContainer(cluster_conf, file_load=False)
        inferred_cpu_cluster_obj = self.platform.load_cluster_by_prop(cluster_props, is_inferred=True)
        return inferred_cpu_cluster_obj
