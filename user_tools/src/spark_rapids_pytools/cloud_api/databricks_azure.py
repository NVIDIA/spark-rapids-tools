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

from dataclasses import dataclass, field
from typing import Any

from spark_rapids_pytools.cloud_api.azurestorage import AzureStorageDriver
from spark_rapids_pytools.cloud_api.sp_types import CloudPlatform, CMDDriverBase, ClusterBase, ClusterNode, \
    PlatformBase, SysInfo, GpuHWInfo, ClusterState, SparkNodeType
from spark_rapids_pytools.common.prop_manager import JSONPropertiesContainer
from spark_rapids_pytools.common.utilities import Utils

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
        return DBAzureCMDDriver(timeout=0, cloud_ctxt=self.ctxt)

    def _install_storage_driver(self):
        self.storage = AzureStorageDriver(self.cli)

    def _construct_cluster_from_props(self, cluster: str, props: str = None):
        return DatabricksAzureCluster(self).set_connection(cluster_id=cluster, props=props)

    def set_offline_cluster(self, cluster_args: dict = None):
        pass

    def migrate_cluster_to_gpu(self, orig_cluster):
        pass

    def create_saving_estimator(self, source_cluster, target_cluster):
        pass

    def create_submission_job(self, job_prop, ctxt) -> Any:
        pass

    def create_local_submission_job(self, job_prop, ctxt) -> Any:
        pass

    def validate_job_submission_args(self, submission_args: dict) -> dict:
        pass


@dataclass
class DBAzureCMDDriver(CMDDriverBase):
    """Represents the command interface that will be used by DATABRICKS_AZURE"""

    def _list_inconsistent_configurations(self) -> list:
        incorrect_envs = super()._list_inconsistent_configurations()
        required_props = self.get_required_props()
        if required_props is not None:
            for prop_entry in required_props:
                prop_value = self.env_vars.get(prop_entry)
                if prop_value is None and prop_entry.startswith('aws_'):
                    incorrect_envs.append('AWS credentials are not set correctly ' +
                                          '(this is required to access resources on S3)')
                    return incorrect_envs
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


@dataclass
class DatabricksAzureNode(ClusterNode):
    """Implementation of Databricks Azure cluster node."""

    region: str = field(default=None, init=False)

    def _pull_and_set_mc_props(self, cli=None):
        src_file_path = Utils.resource_path(f'azure-instances-catalog.json')
        self.mc_props = JSONPropertiesContainer(prop_arg=src_file_path)

    def _set_fields_from_props(self):
        self.name = self.props.get_value_silent('public_dns')
    
    def _pull_sys_info(self, cli=None) -> SysInfo:
        cpu_mem = self.mc_props.get_value(self.instance_type, 'MemoryInfo', 'SizeInMiB')
        # TODO: should we use DefaultVCpus or DefaultCores
        num_cpus = self.mc_props.get_value(self.instance_type, 'VCpuInfo', 'DefaultVCpus')
        return SysInfo(num_cpus=num_cpus, cpu_mem=cpu_mem)

    def _pull_gpu_hw_info(self, cli=None) -> GpuHWInfo or None:
        raw_gpus = self.mc_props.get_value_silent(self.instance_type, 'GpuInfo')
        if raw_gpus is None:
            return None
        # TODO: we assume all gpus of the same type
        raw_gpu_arr = raw_gpus.get('Gpus')
        if raw_gpu_arr is None:
            return None
        raw_gpu = raw_gpu_arr[0]
        gpu_device = GpuDevice.fromstring(raw_gpu['Name'])
        gpu_cnt = raw_gpu['Count']
        gpu_mem = raw_gpu['MemoryInfo']['SizeInMiB']
        return GpuHWInfo(num_gpus=gpu_cnt,
                         gpu_device=gpu_device,
                         gpu_mem=gpu_mem)


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
        # assume that only one master node
        master_nodes_from_conf = self.props.get_value_silent('driver')
        worker_nodes_from_conf = self.props.get_value_silent('executors')
        num_workers = self.props.get_value_silent('num_workers')
        if num_workers is None:
            num_workers = 0
        # construct master node info when cluster is inactive
        if master_nodes_from_conf is None:
            master_node_type_id = self.props.get_value('driver_node_type_id')
            if master_node_type_id is None:
                raise RuntimeError('Failed to find master node information from cluster properties')
            master_nodes_from_conf = {'node_id': None}
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
        master_props = {
            'Id': master_nodes_from_conf['node_id'],
            'props': JSONPropertiesContainer(prop_arg=master_nodes_from_conf, file_load=False),
            # set the node region based on the wrapper defined region
            'region': self.region,
            'instance_type': self.props.get_value('driver_node_type_id')
        }
        master_node = DatabricksAzureNode.create_master_node().set_fields_from_dict(master_props)
        master_node.fetch_and_set_hw_info(self.cli)
        self.nodes = {
            SparkNodeType.WORKER: worker_nodes,
            SparkNodeType.MASTER: master_node
        }

    def _init_connection(self, cluster_id: str = None,
                         props: str = None) -> dict:
        cluster_args = super()._init_connection(cluster_id=cluster_id, props=props)
        # propagate region to the cluster
        cluster_args.setdefault('region', self.cli.get_env_var('region'))
        return cluster_args

    def get_all_spark_properties(self) -> dict:
        return self.props.get_value('spark_conf')

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
