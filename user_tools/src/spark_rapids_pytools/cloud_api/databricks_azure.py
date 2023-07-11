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

"""Implementation specific to DATABRICKS_AZURE"""

import datetime
import json
import os
from dataclasses import dataclass, field
from typing import Any, List

from spark_rapids_pytools.cloud_api.azurestorage import AzureStorageDriver
from spark_rapids_pytools.cloud_api.databricks_azure_job import DBAzureLocalRapidsJob
from spark_rapids_pytools.cloud_api.sp_types import CloudPlatform, CMDDriverBase, ClusterBase, ClusterNode, \
    PlatformBase, SysInfo, GpuHWInfo, ClusterState, SparkNodeType, ClusterGetAccessor, NodeHWInfo, GpuDevice
from spark_rapids_pytools.common.prop_manager import JSONPropertiesContainer
from spark_rapids_pytools.common.sys_storage import FSUtil
from spark_rapids_pytools.common.utilities import Utils
from spark_rapids_pytools.pricing.databricks_azure_pricing import DatabricksAzurePriceProvider
from spark_rapids_pytools.pricing.price_provider import SavingsEstimator


@dataclass
class DBAzurePlatform(PlatformBase):
    """
    Represents the interface and utilities required by DATABRICKS_AZURE.
    Prerequisites:
    - install databricks, azure command lines (databricks cli, azure cli)
    - configure the databricks cli (token, workspace, profile)
    - configure the azure cli
    """
    def __post_init__(self):
        self.type_id = CloudPlatform.DATABRICKS_AZURE
        super().__post_init__()

    def _construct_cli_object(self) -> CMDDriverBase:
        return DBAzureCMDDriver(configs=self.configs, timeout=0, cloud_ctxt=self.ctxt)

    def _install_storage_driver(self):
        self.storage = AzureStorageDriver(self.cli)

    def _construct_cluster_from_props(self, cluster: str, props: str = None):
        return DatabricksAzureCluster(self).set_connection(cluster_id=cluster, props=props)

    def set_offline_cluster(self, cluster_args: dict = None):
        pass

    def migrate_cluster_to_gpu(self, orig_cluster):
        """
        given a cluster, convert it to run NVIDIA Gpu based on mapping instance types
        :param orig_cluster: the original cluster to migrate from
        :return: a new object cluster that supports GPU
        """
        gpu_cluster_ob = DatabricksAzureCluster(self)
        gpu_cluster_ob.migrate_from_cluster(orig_cluster)
        return gpu_cluster_ob

    def create_saving_estimator(self,
                                source_cluster: ClusterGetAccessor,
                                reshaped_cluster: ClusterGetAccessor):
        raw_pricing_config = self.configs.get_value_silent('pricing')
        if raw_pricing_config:
            pricing_config = JSONPropertiesContainer(prop_arg=raw_pricing_config, file_load=False)
        else:
            pricing_config: JSONPropertiesContainer = None
        db_azure_price_provider = DatabricksAzurePriceProvider(region=self.cli.get_region(),
                                                               pricing_configs={'databricks-azure': pricing_config})
        saving_estimator = DBAzureSavingsEstimator(price_provider=db_azure_price_provider,
                                                   reshaped_cluster=reshaped_cluster,
                                                   source_cluster=source_cluster)
        return saving_estimator

    def create_local_submission_job(self, job_prop, ctxt) -> Any:
        return DBAzureLocalRapidsJob(prop_container=job_prop, exec_ctxt=ctxt)

    def validate_job_submission_args(self, submission_args: dict) -> dict:
        pass

    def get_supported_gpus(self) -> dict:
        gpus_from_configs = self.configs.get_value('gpuConfigs', 'user-tools', 'supportedGpuInstances')
        gpu_scopes = {}
        for mc_prof, mc_info in gpus_from_configs.items():
            hw_info_json = mc_info['SysInfo']
            hw_info_ob = SysInfo(num_cpus=hw_info_json['num_cpus'], cpu_mem=hw_info_json['cpu_mem'])
            gpu_info_json = mc_info['GpuInfo']['GPUs'][0]
            gpu_info_obj = GpuHWInfo(num_gpus=gpu_info_json['Count'], gpu_mem=gpu_info_json['MemoryInfo']['SizeInMiB'])
            gpu_scopes[mc_prof] = NodeHWInfo(sys_info=hw_info_ob, gpu_info=gpu_info_obj)
        return gpu_scopes


@dataclass
class DBAzureCMDDriver(CMDDriverBase):
    """Represents the command interface that will be used by DATABRICKS_AZURE"""

    configs: JSONPropertiesContainer = None
    cache_expiration_secs: int = field(default=604800, init=False)  # update the file once a week
    # logger: Logger = field(default=ToolLogging.get_and_setup_logger('rapids.tools.databricks.azure'), init=False)

    def _list_inconsistent_configurations(self) -> list:
        incorrect_envs = super()._list_inconsistent_configurations()
        return incorrect_envs

    def _build_platform_list_cluster(self, cluster, query_args: dict = None) -> list:
        pass

    def pull_cluster_props_by_args(self, args: dict) -> str:
        get_cluster_cmd = ['databricks', 'clusters', 'get']
        if 'Id' in args:
            get_cluster_cmd.extend(['--cluster-id', args.get('Id')])
        elif 'cluster' in args:
            get_cluster_cmd.extend(['--cluster-name', args.get('cluster')])
        else:
            self.logger.error('Invalid arguments to pull the cluster properties')
        return self.run_sys_cmd(get_cluster_cmd)

    def process_instances_description(self, raw_instances_description: str) -> dict:
        processed_instances_description = {}
        instances_description = JSONPropertiesContainer(prop_arg=raw_instances_description, file_load=False)
        for instance in instances_description.props:
            instance_dict = {}
            v_cpus = 0
            memory_gb = 0
            gpus = 0
            if not instance['capabilities']:
                continue
            for item in instance['capabilities']:
                if item['name'] == 'vCPUs':
                    v_cpus = int(item['value'])
                elif item['name'] == 'MemoryGB':
                    memory_gb = int(float(item['value']) * 1024)
                elif item['name'] == 'GPUs':
                    gpus = int(item['value'])
            instance_dict['VCpuInfo'] = {'DefaultVCpus': v_cpus}
            instance_dict['MemoryInfo'] = {'SizeInMiB': memory_gb}
            if gpus > 0:
                gpu_list = [{'Name': '', 'Manufacturer': '', 'Count': gpus, 'MemoryInfo': {'SizeInMiB': 0}}]
                instance_dict['GpuInfo'] = {'GPUs': gpu_list}
            processed_instances_description[instance['name']] = instance_dict
        return processed_instances_description

    def generate_instances_description(self, fpath: str):
        cmd_params = ['az vm list-skus',
                      '--location', f'{self.get_region()}']
        raw_instances_description = self.run_sys_cmd(cmd_params)
        json_instances_description = self.process_instances_description(raw_instances_description)
        with open(fpath, 'w', encoding='UTF-8') as output_file:
            json.dump(json_instances_description, output_file, indent=2)

    def _build_platform_describe_node_instance(self, node: ClusterNode) -> list:
        pass

    def _caches_expired(self, cache_file) -> bool:
        if not os.path.exists(cache_file):
            return True
        modified_time = os.path.getmtime(cache_file)
        diff_time = int(datetime.datetime.now().timestamp() - modified_time)
        if diff_time > self.cache_expiration_secs:
            return True
        return False

    def init_instances_description(self) -> str:
        cache_dir = Utils.get_rapids_tools_env('CACHE_FOLDER')
        fpath = FSUtil.build_path(cache_dir, 'azure-instances-catalog.json')
        if self._caches_expired(fpath):
            self.logger.info('Downloading the Azure instance type descriptions catalog')
            self.generate_instances_description(fpath)
        else:
            self.logger.info('The Azure instance type descriptions catalog is loaded from the cache')
        return fpath

    def get_submit_spark_job_cmd_for_cluster(self, cluster_name: str, submit_args: dict) -> List[str]:
        raise NotImplementedError

    def get_region(self) -> str:
        if self.env_vars.get('location'):
            return self.env_vars.get('location')
        return self.env_vars.get('region')


@dataclass
class DatabricksAzureNode(ClusterNode):
    """Implementation of Databricks Azure cluster node."""

    region: str = field(default=None, init=False)

    def _pull_and_set_mc_props(self, cli=None):
        instances_description_path = cli.init_instances_description()
        self.mc_props = JSONPropertiesContainer(prop_arg=instances_description_path)

    def _set_fields_from_props(self):
        self.name = self.props.get_value_silent('public_dns')

    def _pull_sys_info(self, cli=None) -> SysInfo:
        cpu_mem = self.mc_props.get_value(self.instance_type, 'MemoryInfo', 'SizeInMiB')
        # TODO: should we use DefaultVCpus or DefaultCores
        num_cpus = self.mc_props.get_value(self.instance_type, 'VCpuInfo', 'DefaultVCpus')

        return SysInfo(num_cpus=num_cpus, cpu_mem=cpu_mem)

    def _pull_gpu_hw_info(self, cli=None) -> GpuHWInfo or None:
        gpu_info = cli.configs.get_value('gpuConfigs', 'user-tools', 'supportedGpuInstances')
        if gpu_info is None:
            return None
        if self.instance_type not in gpu_info:
            return None
        gpu_instance = gpu_info[self.instance_type]['GpuInfo']['GPUs'][0]
        gpu_device = GpuDevice.fromstring(gpu_instance['Name'])
        return GpuHWInfo(num_gpus=gpu_instance['Count'],
                         gpu_device=gpu_device,
                         gpu_mem=gpu_instance['MemoryInfo']['SizeInMiB'])


@dataclass
class DatabricksAzureCluster(ClusterBase):
    """
    Represents an instance of running cluster on Databricks.
    """

    def _set_fields_from_props(self):
        super()._set_fields_from_props()
        self.uuid = self.props.get_value('cluster_id')
        self.state = ClusterState.fromstring(self.props.get_value('state'))

    def _set_name_from_props(self) -> None:
        self.name = self.props.get_value('cluster_name')

    def _init_nodes(self):
        # assume that only one driver node
        driver_nodes_from_conf = self.props.get_value_silent('driver')
        worker_nodes_from_conf = self.props.get_value_silent('executors')
        num_workers = self.props.get_value_silent('num_workers')
        if num_workers is None:
            num_workers = 0
        # construct driver node info when cluster is inactive
        if driver_nodes_from_conf is None:
            driver_node_type_id = self.props.get_value('driver_node_type_id')
            if driver_node_type_id is None:
                raise RuntimeError('Failed to find driver node information from cluster properties')
            driver_nodes_from_conf = {'node_id': None}
        # construct worker nodes info when cluster is inactive
        if worker_nodes_from_conf is None:
            worker_node_type_id = self.props.get_value('node_type_id')
            if worker_node_type_id is None:
                raise RuntimeError('Failed to find worker node information from cluster properties')
            worker_nodes_from_conf = [{'node_id': None} for i in range(num_workers)]
        # create workers array
        worker_nodes: list = []
        for worker_node in worker_nodes_from_conf:
            worker_props = {
                'Id': worker_node['node_id'],
                'props': JSONPropertiesContainer(prop_arg=worker_node, file_load=False),
                # set the node region based on the wrapper defined region
                'region': self.region,
                'instance_type': self.props.get_value('node_type_id')
            }
            worker = DatabricksAzureNode.create_worker_node().set_fields_from_dict(worker_props)
            worker.fetch_and_set_hw_info(self.cli)
            worker_nodes.append(worker)
        driver_props = {
            'Id': driver_nodes_from_conf['node_id'],
            'props': JSONPropertiesContainer(prop_arg=driver_nodes_from_conf, file_load=False),
            # set the node region based on the wrapper defined region
            'region': self.region,
            'instance_type': self.props.get_value('driver_node_type_id')
        }
        driver_node = DatabricksAzureNode.create_master_node().set_fields_from_dict(driver_props)
        driver_node.fetch_and_set_hw_info(self.cli)
        self.nodes = {
            SparkNodeType.WORKER: worker_nodes,
            SparkNodeType.MASTER: driver_node
        }

    def _init_connection(self, cluster_id: str = None,
                         props: str = None) -> dict:
        cluster_args = super()._init_connection(cluster_id=cluster_id, props=props)
        # propagate region to the cluster
        cluster_args.setdefault('region', self.cli.get_env_var('region'))
        return cluster_args

    def get_all_spark_properties(self) -> dict:
        return self.props.get_value_silent('spark_conf')

    def _build_migrated_cluster(self, orig_cluster):
        """
        specific to the platform on how to build a cluster based on migration
        :param orig_cluster: the cpu_cluster that does not support the GPU devices.
        """
        # get the map of the instance types
        mc_type_map, _ = orig_cluster.find_matches_for_node()
        new_worker_nodes: list = []
        for anode in orig_cluster.nodes.get(SparkNodeType.WORKER):
            # loop on all worker nodes.
            # even if the node is the same type, we still need to set the hardware
            if anode.instance_type not in mc_type_map:
                # the node stays the same
                # skip converting the node
                new_instance_type = anode.instance_type
                self.logger.info('Node with %s supports GPU devices.',
                                 anode.instance_type)
            else:
                new_instance_type = mc_type_map.get(anode.instance_type)
                self.logger.info('Converting node %s into GPU supported instance-type %s',
                                 anode.instance_type,
                                 new_instance_type)
            worker_props = {
                'instance_type': new_instance_type,
                'name': anode.name,
                'Id': anode.Id,
                'region': anode.region,
                'props': anode.props,
            }
            new_node = DatabricksAzureNode.create_worker_node().set_fields_from_dict(worker_props)
            new_worker_nodes.append(new_node)
        self.nodes = {
            SparkNodeType.WORKER: new_worker_nodes,
            SparkNodeType.MASTER: orig_cluster.nodes.get(SparkNodeType.MASTER)
        }
        if bool(mc_type_map):
            # update the platform notes
            self.platform.update_ctxt_notes('nodeConversions', mc_type_map)

    def get_tmp_storage(self) -> str:
        raise NotImplementedError


@dataclass
class DBAzureSavingsEstimator(SavingsEstimator):
    """
    A class that calculates the savings based on a Databricks-Azure price provider
    """

    def _get_cost_per_cluster(self, cluster: ClusterGetAccessor):
        db_azure_cost = 0.0
        for node_type in [SparkNodeType.MASTER, SparkNodeType.WORKER]:
            instance_type = cluster.get_node_instance_type(node_type)
            nodes_cnt = cluster.get_nodes_cnt(node_type)
            cost = self.price_provider.get_instance_price(instance=instance_type)
            db_azure_cost += cost * nodes_cnt
        return db_azure_cost

    def _setup_costs(self):
        # calculate target_cost
        self.target_cost = self._get_cost_per_cluster(self.reshaped_cluster)
        self.source_cost = self._get_cost_per_cluster(self.source_cluster)
