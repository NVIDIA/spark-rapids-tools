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


"""Implementation specific to OnPrem"""

from dataclasses import dataclass
from typing import Any, List

from spark_rapids_pytools.rapids.rapids_job import RapidsLocalJob
from spark_rapids_pytools.cloud_api.sp_types import PlatformBase, ClusterBase, ClusterNode, \
    CMDDriverBase, CloudPlatform, ClusterGetAccessor, GpuDevice, \
    GpuHWInfo, NodeHWInfo, SparkNodeType, SysInfo
from spark_rapids_pytools.common.prop_manager import JSONPropertiesContainer
from spark_rapids_pytools.common.sys_storage import StorageDriver
from spark_rapids_pytools.pricing.dataproc_pricing import DataprocPriceProvider
from spark_rapids_pytools.pricing.price_provider import SavingsEstimator


@dataclass
class OnPremPlatform(PlatformBase):
    """
    Represents the interface and utilities required by OnPrem platform.
    """

    def __post_init__(self):
        self.type_id = CloudPlatform.ONPREM
        self.platform = self.ctxt_args.get('targetPlatform')
        super().__post_init__()

    def _construct_cli_object(self):
        return CMDDriverBase(timeout=0, cloud_ctxt=self.ctxt)

    def _install_storage_driver(self):
        self.storage = OnPremStorageDriver(self.cli)

    def create_local_submission_job(self, job_prop, ctxt) -> Any:
        return OnPremLocalRapidsJob(prop_container=job_prop, exec_ctxt=ctxt)

    def _construct_cluster_from_props(self, cluster: str, props: str = None):
        if self.platform is not None:
            onprem_cluster = OnPremCluster(self).set_connection(cluster_id=cluster, props=props)
            return onprem_cluster
        return None

    def migrate_cluster_to_gpu(self, orig_cluster):
        """
            given a cluster, convert it to run NVIDIA Gpu based on mapping instance types
            :param orig_cluster: the original cluster to migrate from
            :return: a new object cluster that supports GPU.
        """
        if orig_cluster is not None:
            gpu_cluster_ob = OnPremCluster(self)
            gpu_cluster_ob.migrate_from_cluster(orig_cluster)
            return gpu_cluster_ob
        return orig_cluster

    def get_platform_name(self) -> str:
        """
        This used to get the lower case of the platform of the runtime.
        :return: the name of the platform of the runtime in lower_case.
        """
        if self.platform is not None:
            if self.platform == 'dataproc':
                self_id = CloudPlatform.DATAPROC
        else:
            self_id = self.type_id
        return CloudPlatform.pretty_print(self_id)

    def get_footer_message(self) -> str:
        return 'To support acceleration with T4 GPUs, please use these worker node instance types.'

    def create_saving_estimator(self,
                                source_cluster: ClusterGetAccessor,
                                reshaped_cluster: ClusterGetAccessor):
        if self.platform == 'dataproc':
            region = 'us-central1'
            raw_pricing_config = self.configs.get_value_silent('csp_pricing')
            if raw_pricing_config:
                pricing_config = JSONPropertiesContainer(prop_arg=raw_pricing_config,
                                                         file_load=False)
            else:
                pricing_config: JSONPropertiesContainer = None
            pricing_provider = DataprocPriceProvider(region=region,
                                                     pricing_configs={'gcloud': pricing_config})
            saving_estimator = OnpremSavingsEstimator(price_provider=pricing_provider,
                                                      reshaped_cluster=reshaped_cluster,
                                                      source_cluster=source_cluster)
        return saving_estimator

    def set_offline_cluster(self, cluster_args: dict = None):
        pass

    def validate_job_submission_args(self, submission_args: dict) -> dict:
        pass

    def get_supported_gpus(self) -> dict:
        def calc_num_gpus(gpus_criteria_conf: List[dict], num_cores: int) -> int:
            if gpus_criteria_conf:
                for c_conf in gpus_criteria_conf:
                    if c_conf.get('lowerBound') <= num_cores < c_conf.get('upperBound'):
                        return c_conf.get('gpuCount')
            # Use default if the configuration is not loaded. This should not happen anyway.
            return 2 if num_cpu >= 16 else 1

        gpus_from_configs = self.configs.get_value('gpuConfigs', 'dataproc', 'user-tools', 'supportedGpuInstances')
        gpu_count_criteria = self.configs.get_value('gpuConfigs', 'dataproc', 'user-tools',
                                                    'gpuPerMachine', 'criteria', 'numCores')
        gpu_scopes = {}
        for mc_prof, mc_info in gpus_from_configs.items():
            unit_info = mc_info['seriesInfo']
            for num_cpu in unit_info['vCPUs']:
                prof_name = f'{mc_prof}-{num_cpu}'
                # create the sys info
                memory_mb = num_cpu * unit_info['memPerCPU']
                sys_info_obj = SysInfo(num_cpus=num_cpu, cpu_mem=memory_mb)
                # create gpu_info
                gpu_cnt = calc_num_gpus(gpu_count_criteria, num_cpu)
                # default memory
                gpu_device = GpuDevice.get_default_gpu()
                gpu_mem = gpu_device.get_gpu_mem()[0]
                gpu_info_obj = GpuHWInfo(num_gpus=gpu_cnt, gpu_mem=gpu_mem, gpu_device=gpu_device)
                gpu_scopes[prof_name] = NodeHWInfo(sys_info=sys_info_obj, gpu_info=gpu_info_obj)
        return gpu_scopes


@dataclass
class OnPremStorageDriver(StorageDriver):
    cli: CMDDriverBase


@dataclass
class OnPremLocalRapidsJob(RapidsLocalJob):
    """
    Implementation of a RAPIDS job that runs on a local machine.
    """
    job_label = 'onpremLocal'


@dataclass
class OnPremNode(ClusterNode):
    """Implementation of Onprem cluster node."""

    def fetch_and_set_hw_info(self, cli=None):
        sys_info = self._pull_sys_info(cli)
        self.construct_hw_info(cli=cli, sys_info=sys_info)

    def _pull_sys_info(self, cli=None) -> SysInfo:
        cpu_mem = self.props.get_value('memory')
        cpu_mem = cpu_mem.replace('MiB', '')
        num_cpus = self.props.get_value('numCores')
        return SysInfo(num_cpus=num_cpus, cpu_mem=cpu_mem)

    def _get_dataproc_nearest_cpu_cores(self, num_cores):
        if num_cores == 1:
            cpu_cores = 1
        elif num_cores == 2:
            cpu_cores = 2
        elif 3 <= num_cores <= 4:
            cpu_cores = 4
        elif 5 <= num_cores <= 8:
            cpu_cores = 8
        elif 9 <= num_cores <= 16:
            cpu_cores = 16
        elif 17 <= num_cores <= 32:
            cpu_cores = 32
        elif 33 <= num_cores <= 64:
            cpu_cores = 64
        else:
            cpu_cores = 96
        return cpu_cores

    def _get_instance_type(self, platform_name=None):
        instance_type = None
        if platform_name == 'dataproc':
            cpu_cores = self.props.get_value('numCores')
            cpu_cores = self._get_dataproc_nearest_cpu_cores(cpu_cores)
            instance_type = 'n1-standard-' + str(cpu_cores)
        return instance_type

    def _set_fields_from_props(self):
        # set the machine type
        if not self.props:
            return
        if self.platform_name is not None:
            self.instance_type = self._get_instance_type(self.platform_name)

    def _pull_gpu_hw_info(self, cli=None) -> None:
        pass


@dataclass
class OnPremCluster(ClusterBase):
    """
    Represents an instance of running cluster on OnPrem platform.
    """

    def _init_nodes(self):
        raw_worker_prop = self.props.get_value_silent('config', 'workerConfig')
        worker_nodes: list = []
        if raw_worker_prop:
            worker_nodes_total = self.props.get_value('config', 'workerConfig', 'numWorkers')
            for i in range(worker_nodes_total):
                worker_props = {
                    'name': 'worker' + str(i),
                    'props': JSONPropertiesContainer(prop_arg=raw_worker_prop, file_load=False),
                    # set the node zone based on the wrapper defined zone
                    'zone': self.zone,
                    'platform_name': self.platform.get_platform_name()
                }
                worker = OnPremNode.create_worker_node().set_fields_from_dict(worker_props)
                # TODO for optimization, we should set HW props for 1 worker
                worker.fetch_and_set_hw_info(self.cli)
                worker_nodes.append(worker)
        raw_master_props = self.props.get_value('config', 'masterConfig')
        master_props = {
            'name': 'master',
            'props': JSONPropertiesContainer(prop_arg=raw_master_props, file_load=False),
            # set the node zone based on the wrapper defined zone
            'zone': self.zone,
            'platform_name': self.platform.get_platform_name()
        }

        master_node = OnPremNode.create_master_node().set_fields_from_dict(master_props)
        master_node.fetch_and_set_hw_info(self.cli)
        self.nodes = {
            SparkNodeType.WORKER: worker_nodes,
            SparkNodeType.MASTER: master_node
        }

    def _build_migrated_cluster(self, orig_cluster):
        """
        specific to the platform on how to build a cluster based on migration
        :param orig_cluster: the cpu_cluster
        """
        # get the map of the instance types
        _, supported_mc_map = orig_cluster.find_matches_for_node()
        new_worker_nodes: list = []
        for anode in orig_cluster.nodes.get(SparkNodeType.WORKER):
            new_instance_type = anode.instance_type
            worker_props = {
                'instance_type': new_instance_type,
                'name': anode.name,
                'zone': anode.zone,
            }
            new_node = OnPremNode.create_worker_node().set_fields_from_dict(worker_props)
            gpu_mc_hw: ClusterNode = supported_mc_map.get(new_instance_type)
            new_node.construct_hw_info(cli=None,
                                       gpu_info=gpu_mc_hw.gpu_info,
                                       sys_info=gpu_mc_hw.sys_info)
            new_worker_nodes.append(new_node)
        master_node = orig_cluster.nodes.get(SparkNodeType.MASTER)
        self.nodes = {
            SparkNodeType.WORKER: new_worker_nodes,
            SparkNodeType.MASTER: orig_cluster.nodes.get(SparkNodeType.MASTER)
        }
        # force filling mc_type_map for on_prem platform.
        mc_type_map = {
            'Driver node': master_node.instance_type,
            'Worker node': new_worker_nodes[0].instance_type
        }
        self.platform.update_ctxt_notes('nodeConversions', mc_type_map)

    def _set_render_args_create_template(self) -> dict:
        pass

    def get_all_spark_properties(self) -> dict:
        pass

    def get_tmp_storage(self) -> str:
        pass


@dataclass
class OnpremSavingsEstimator(SavingsEstimator):
    """
    A class that calculates the savings based on Onprem price provider
    """
    def __calculate_dataproc_group_cost(self, cluster_inst: ClusterGetAccessor, node_type: SparkNodeType):
        nodes_cnt = cluster_inst.get_nodes_cnt(node_type)
        cores_count = cluster_inst.get_node_core_count(node_type)
        mem_mb = cluster_inst.get_node_mem_mb(node_type)
        node_mc_type = cluster_inst.get_node_instance_type(node_type)
        # memory here is in mb, we need to convert it to gb
        mem_gb = float(mem_mb) / 1024

        cores_cost = self.price_provider.get_cpu_price(node_mc_type) * int(cores_count)
        memory_cost = self.price_provider.get_ram_price(node_mc_type) * mem_gb
        # calculate the GPU cost
        gpu_per_machine, gpu_type = cluster_inst.get_gpu_per_node(node_type)
        gpu_cost = 0.0
        if gpu_per_machine > 0:
            gpu_unit_price = self.price_provider.get_gpu_price(gpu_type)
            gpu_cost = gpu_unit_price * gpu_per_machine
        return nodes_cnt * (cores_cost + memory_cost + gpu_cost)

    def _get_cost_per_cluster(self, cluster: ClusterGetAccessor):
        if self.price_provider.name.casefold() == 'dataproc':
            master_cost = self.__calculate_dataproc_group_cost(cluster, SparkNodeType.MASTER)
            workers_cost = self.__calculate_dataproc_group_cost(cluster, SparkNodeType.WORKER)
            dataproc_cost = self.price_provider.get_container_cost()
            total_cost = master_cost + workers_cost + dataproc_cost
        return total_cost

    def _setup_costs(self):
        # calculate target_cost
        self.target_cost = self._get_cost_per_cluster(self.reshaped_cluster)
        self.source_cost = self._get_cost_per_cluster(self.source_cluster)
