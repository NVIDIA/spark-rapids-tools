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


"""Implementation specific to Dataproc"""

import json
from dataclasses import dataclass, field
from typing import Any, List

from spark_rapids_pytools.cloud_api.dataproc_job import DataprocLocalRapidsJob
from spark_rapids_pytools.cloud_api.gstorage import GStorageDriver
from spark_rapids_pytools.cloud_api.sp_types import PlatformBase, CMDDriverBase, CloudPlatform, \
    ClusterBase, ClusterNode, SysInfo, GpuHWInfo, SparkNodeType, ClusterState, GpuDevice, \
    NodeHWInfo, ClusterGetAccessor
from spark_rapids_pytools.common.prop_manager import JSONPropertiesContainer
from spark_rapids_pytools.common.sys_storage import FSUtil
from spark_rapids_pytools.common.utilities import SysCmd, Utils
from spark_rapids_pytools.pricing.dataproc_pricing import DataprocPriceProvider
from spark_rapids_pytools.pricing.price_provider import SavingsEstimator


@dataclass
class DataprocPlatform(PlatformBase):
    """
    Represents the interface and utilities required by Dataproc.
    Prerequisites:
    - install gcloud command lines (gcloud, gsutil)
    - configure the gcloud CLI.
    - dataproc has staging temporary storage. we can retrieve that from the cluster properties.
    """

    def __post_init__(self):
        self.type_id = CloudPlatform.DATAPROC
        super().__post_init__()

    def _set_remaining_configuration_list(self) -> None:
        remaining_props = self._get_config_environment('loadedConfigProps')
        if not remaining_props:
            return
        properties_map_arr = self._get_config_environment('cliConfig',
                                                          'confProperties',
                                                          'propertiesMap')
        if properties_map_arr:
            config_cmd_prefix = ['gcloud', 'config', 'get']
            for prop_entry in properties_map_arr:
                prop_entry_key = prop_entry.get('propKey')
                if self.ctxt.get(prop_entry_key):
                    # Skip if the property already set
                    continue
                prop_cmd = config_cmd_prefix[:]
                prop_cmd.append(f'{prop_entry.get("section")}/{prop_entry_key}')
                cmd_args = {
                    'cmd': prop_cmd,
                }
                prop_cmd_obj = SysCmd().build(cmd_args)
                prop_cmd_res = prop_cmd_obj.exec()
                if prop_cmd_res:
                    self.ctxt.update({prop_entry_key: prop_cmd_res})
            for prop_entry in properties_map_arr:
                prop_entry_key = prop_entry.get('propKey')
                if self.ctxt.get(prop_entry_key) is None:
                    # set it using environment variable if possible
                    self._set_env_prop_from_env_var(prop_entry_key)

    def _construct_cli_object(self) -> CMDDriverBase:
        return DataprocCMDDriver(timeout=0, cloud_ctxt=self.ctxt)

    def _install_storage_driver(self):
        self.storage = GStorageDriver(self.cli)

    def _construct_cluster_from_props(self, cluster: str, props: str = None):
        return DataprocCluster(self).set_connection(cluster_id=cluster, props=props)

    def set_offline_cluster(self, cluster_args: dict = None):
        pass

    def migrate_cluster_to_gpu(self, orig_cluster):
        """
        given a cluster, convert it to run NVIDIA Gpu based on mapping instance types
        :param orig_cluster: the original cluster to migrate from
        :return: a new object cluster that supports GPU.
        """
        gpu_cluster_ob = DataprocCluster(self)
        gpu_cluster_ob.migrate_from_cluster(orig_cluster)
        return gpu_cluster_ob

    def create_saving_estimator(self,
                                source_cluster: ClusterGetAccessor,
                                reshaped_cluster: ClusterGetAccessor):
        raw_pricing_config = self.configs.get_value_silent('pricing')
        if raw_pricing_config:
            pricing_config = JSONPropertiesContainer(prop_arg=raw_pricing_config,
                                                     file_load=False)
        else:
            pricing_config: JSONPropertiesContainer = None
        pricing_provider = DataprocPriceProvider(region=self.cli.get_region(),
                                                 pricing_configs={'gcloud': pricing_config})
        saving_estimator = DataprocSavingsEstimator(price_provider=pricing_provider,
                                                    reshaped_cluster=reshaped_cluster,
                                                    source_cluster=source_cluster)
        return saving_estimator

    def create_local_submission_job(self, job_prop, ctxt) -> Any:
        return DataprocLocalRapidsJob(prop_container=job_prop, exec_ctxt=ctxt)

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

        gpus_from_configs = self.configs.get_value('gpuConfigs', 'user-tools', 'supportedGpuInstances')
        gpu_count_criteria = self.configs.get_value('gpuConfigs',
                                                    'user-tools',
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
class DataprocCMDDriver(CMDDriverBase):
    """Represents the command interface that will be used by Dataproc"""

    def _list_inconsistent_configurations(self) -> list:
        incorrect_envs = super()._list_inconsistent_configurations()
        required_props = self.get_required_props()
        if required_props:
            for prop_entry in required_props:
                prop_value = self.env_vars.get(prop_entry)
                if prop_value is None:
                    incorrect_envs.append(f'Property {prop_entry} is not set.')
        return incorrect_envs

    def _build_platform_describe_node_instance(self, node: ClusterNode) -> list:
        cmd_params = ['gcloud',
                      'compute',
                      'machine-types',
                      'describe',
                      f'{node.instance_type}',
                      '--zone',
                      f'{node.zone}']
        return cmd_params

    def _build_platform_list_cluster(self,
                                     cluster,
                                     query_args: dict = None) -> list:
        cmd_params = ['gcloud', 'dataproc', 'clusters', 'list',
                      f"--region='{self.get_region()}'"]
        filter_args = [f'clusterName = {cluster.name}']
        if query_args is not None:
            if 'state' in query_args:
                state_param = query_args.get('state')
                filter_args.append(f'status.state = {state_param}')
        filter_arg = Utils.gen_joined_str(' AND ', filter_args)
        cmd_params.append(f"--filter='{filter_arg}'")
        return cmd_params

    def pull_cluster_props_by_args(self, args: dict) -> str:
        cluster_name = args.get('cluster')
        # TODO: We should piggyback on the cmd so that we do not have to add region in each cmd
        # region is already set in the instance
        if 'region' in args:
            region_name = args.get('region')
        else:
            region_name = self.get_region()
        describe_cluster_cmd = ['gcloud',
                                'dataproc',
                                'clusters',
                                'describe',
                                cluster_name,
                                '--region',
                                region_name]
        return self.run_sys_cmd(describe_cluster_cmd)

    def exec_platform_describe_accelerator(self,
                                           accelerator_type: str,
                                           **cmd_args) -> str:
        cmd_args = ['gcloud', 'compute', 'accelerator-types', 'describe',
                    accelerator_type, '--zone',
                    self.get_env_var('zone')]
        return self.run_sys_cmd(cmd_args)

    def _build_ssh_cmd_prefix_for_node(self, node: ClusterNode) -> str:
        pref_args = ['gcloud',
                     'compute', 'ssh',
                     node.name,
                     '--zone',
                     self.get_env_var('zone'),
                     '--command=']
        return Utils.gen_joined_str(' ', pref_args)

    def _build_cmd_scp_to_node(self, node: ClusterNode, src: str, dest: str) -> str:
        pref_args = ['gcloud',
                     'compute', 'scp',
                     '--zone',
                     self.get_env_var('zone'),
                     src,
                     f'{node.name}:{dest}']
        return Utils.gen_joined_str(' ', pref_args)

    def _build_cmd_scp_from_node(self, node: ClusterNode, src: str, dest: str) -> str:
        pref_args = ['gcloud',
                     'compute', 'scp',
                     '--zone',
                     self.get_env_var('zone'),
                     f'{node.name}:{src}',
                     dest]
        return Utils.gen_joined_str(' ', pref_args)

    def _construct_ssh_cmd_with_prefix(self, prefix: str, remote_cmd: str) -> str:
        # for dataproc, the remote should not be preceded by ws
        return f'{prefix}{remote_cmd}'

    def get_submit_spark_job_cmd_for_cluster(self,
                                             cluster_name: str,
                                             submit_args: dict) -> List[str]:
        cmd = ['gcloud',
               'dataproc',
               'jobs',
               'submit',
               'spark',
               '--cluster',
               cluster_name,
               '--region',
               self.get_region()]
        # add the platform arguments: jars, class
        if 'platformSparkJobArgs' in submit_args:
            for arg_k, arg_val in submit_args.get('platformSparkJobArgs').items():
                if arg_val:
                    cmd.append(f'--{arg_k}={arg_val}')
        # add the jar arguments
        jar_args = submit_args.get('jarArgs')
        if jar_args:
            cmd.append('--')
            # expects a list of string
            cmd.extend(jar_args)
        return cmd


@dataclass
class DataprocNode(ClusterNode):
    """Implementation of Dataproc cluster node."""

    zone: str = field(default=None, init=False)

    @staticmethod
    def __extract_info_from_value(conf_val: str):
        if '/' in conf_val:
            # this is a valid url-path
            return FSUtil.get_resource_name(conf_val)
        # this is a value
        return conf_val

    def _pull_gpu_hw_info(self, cli=None) -> GpuHWInfo:
        # https://cloud.google.com/compute/docs/gpus
        # the gpu info is not included in the instance type
        # we need to:
        # 1- get the accelerator of the node if any
        #    "gcloud compute accelerator-types describe nvidia-tesla-a100 --zone=us-central1-a"
        # 2- Read the description flag to determine the memory size. (applies for A100)
        #    If it is not included, then load the gpu-memory from a lookup table
        def parse_accelerator_description(raw_description: str) -> dict:
            parsing_res = {}
            descr_json = json.loads(raw_description)
            description_field = descr_json.get('description')
            field_components = description_field.split()
            # filter out non-used tokens
            dumped_tokens = ['NVIDIA', 'Tesla']
            final_entries = [entry.lower() for entry in field_components if entry not in dumped_tokens]
            gpu_device: GpuDevice = None
            for token_entry in final_entries:
                if 'GB' in token_entry:
                    # this is the memory value
                    memory_in_gb_str = token_entry.removesuffix('GB')
                    gpu_mem = 1024 * int(memory_in_gb_str)
                    parsing_res.setdefault('gpu_mem', gpu_mem)
                else:
                    gpu_device = GpuDevice.fromstring(token_entry)
                    parsing_res.setdefault('gpu_device', gpu_device)
            if 'gpu_mem' not in parsing_res:
                # get the GPU memory size from lookup
                parsing_res.setdefault('gpu_mem', gpu_device.get_gpu_mem()[0])
            return parsing_res

        accelerator_arr = self.props.get_value_silent('accelerators')
        if not accelerator_arr:
            return None

        for defined_acc in accelerator_arr:
            # TODO: if the accelerator_arr has other non-gpu ones, then we need to loop until we
            #       find the gpu accelerators
            gpu_configs = {'num_gpus': defined_acc.get('acceleratorCount')}
            accelerator_type = defined_acc.get('acceleratorTypeUri')
            gpu_device_type = self.__extract_info_from_value(accelerator_type)
            gpu_description = cli.exec_platform_describe_accelerator(accelerator_type=gpu_device_type,
                                                                     cmd_args=None)
            extra_gpu_info = parse_accelerator_description(gpu_description)
            gpu_configs.update(extra_gpu_info)
            return GpuHWInfo(num_gpus=gpu_configs.get('num_gpus'),
                             gpu_device=gpu_configs.get('gpu_device'),
                             gpu_mem=gpu_configs.get('gpu_mem'))

    def _pull_sys_info(self, cli=None) -> SysInfo:
        cpu_mem = self.mc_props.get_value('memoryMb')
        num_cpus = self.mc_props.get_value('guestCpus')
        return SysInfo(num_cpus=num_cpus, cpu_mem=cpu_mem)

    def _pull_and_set_mc_props(self, cli=None):
        instance_description = cli.exec_platform_describe_node_instance(self)
        self.mc_props = JSONPropertiesContainer(prop_arg=instance_description, file_load=False)

    def _set_fields_from_props(self):
        # set the machine type
        if not self.props:
            return
        mc_type_uri = self.props.get_value('machineTypeUri')
        if mc_type_uri:
            self.instance_type = self.__extract_info_from_value(mc_type_uri)
        else:
            # check if the machine type is  under a different name
            mc_type = self.props.get_value('machineType')
            if mc_type:
                self.instance_type = self.__extract_info_from_value(mc_type)


@dataclass
class DataprocCluster(ClusterBase):
    """
    Represents an instance of running cluster on Dataproc.
    """

    def _get_temp_gs_storage(self) -> str:
        temp_bucket = self.props.get_value_silent('config', 'tempBucket')
        if temp_bucket:
            return f'gs://{temp_bucket}/{self.uuid}'
        return None

    def get_eventlogs_from_config(self) -> List[str]:
        res_arr = super().get_eventlogs_from_config()
        if not res_arr:
            # The SHS was not set for the cluster. Use the tmp bucket storage as the default SHS log directory
            # append the temporary gstorage followed by the SHS folder
            tmp_gs = self._get_temp_gs_storage()
            res_arr.append(f'{tmp_gs}/spark-job-history')
        return res_arr

    def _set_fields_from_props(self):
        super()._set_fields_from_props()
        self.uuid = self.props.get_value('clusterUuid')
        self.state = ClusterState.fromstring(self.props.get_value('status', 'state'))

    def _set_name_from_props(self) -> None:
        self.name = self.props.get_value('clusterName')

    def _init_nodes(self):
        # assume that only one master node
        master_nodes_from_conf = self.props.get_value('config', 'masterConfig', 'instanceNames')
        raw_worker_prop = self.props.get_value_silent('config', 'workerConfig')
        worker_nodes: list = []
        if raw_worker_prop:
            worker_nodes_from_conf = self.props.get_value('config', 'workerConfig', 'instanceNames')
            # create workers array
            for worker_node in worker_nodes_from_conf:
                worker_props = {
                    'name': worker_node,
                    'props': JSONPropertiesContainer(prop_arg=raw_worker_prop, file_load=False),
                    # set the node zone based on the wrapper defined zone
                    'zone': self.zone
                }
                worker = DataprocNode.create_worker_node().set_fields_from_dict(worker_props)
                # TODO for optimization, we should set HW props for 1 worker
                worker.fetch_and_set_hw_info(self.cli)
                worker_nodes.append(worker)
        raw_master_props = self.props.get_value('config', 'masterConfig')
        master_props = {
            'name': master_nodes_from_conf[0],
            'props': JSONPropertiesContainer(prop_arg=raw_master_props, file_load=False),
            # set the node zone based on the wrapper defined zone
            'zone': self.zone
        }
        master_node = DataprocNode.create_master_node().set_fields_from_dict(master_props)
        master_node.fetch_and_set_hw_info(self.cli)
        self.nodes = {
            SparkNodeType.WORKER: worker_nodes,
            SparkNodeType.MASTER: master_node
        }

    def _init_connection(self, cluster_id: str = None,
                         props: str = None) -> dict:
        cluster_args = super()._init_connection(cluster_id=cluster_id, props=props)
        # propagate zone to the cluster
        cluster_args.setdefault('zone', self.cli.get_env_var('zone'))
        return cluster_args

    def _build_migrated_cluster(self, orig_cluster):
        """
        specific to the platform on how to build a cluster based on migration
        :param orig_cluster: the cpu_cluster that does not support the GPU devices.
        """
        # get the map of the instance types
        mc_type_map, supported_mc_map = orig_cluster.find_matches_for_node()
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
                'zone': anode.zone,
            }
            new_node = DataprocNode.create_worker_node().set_fields_from_dict(worker_props)
            # we cannot rely on setting gpu info from the SDK because
            # dataproc does not bind machine types to GPUs
            # new_node.fetch_and_set_hw_info(self.cli)
            gpu_mc_hw: ClusterNode = supported_mc_map.get(new_instance_type)
            new_node.construct_hw_info(cli=None,
                                       gpu_info=gpu_mc_hw.gpu_info,
                                       sys_info=gpu_mc_hw.sys_info)
            new_worker_nodes.append(new_node)
        self.nodes = {
            SparkNodeType.WORKER: new_worker_nodes,
            SparkNodeType.MASTER: orig_cluster.nodes.get(SparkNodeType.MASTER)
        }
        if bool(mc_type_map):
            # update the platform notes
            self.platform.update_ctxt_notes('nodeConversions', mc_type_map)

    def get_all_spark_properties(self) -> dict:
        """Returns a dictionary containing the spark configurations defined in the cluster properties"""
        sw_props = self.props.get_value_silent('config', 'softwareConfig', 'properties')
        if sw_props:
            k_prefix = 'spark:'
            return {key[len(k_prefix):]: value for (key, value) in sw_props.items() if key.startswith(k_prefix)}
        return {}

    def get_tmp_storage(self) -> str:
        return self._get_temp_gs_storage()

    def get_image_version(self) -> str:
        return self.props.get_value_silent('config', 'softwareConfig', 'imageVersion')

    def _set_render_args_create_template(self) -> dict:
        worker_node = self.get_worker_node()
        gpu_per_machine, gpu_device = self.get_gpu_per_worker()
        # map the gpu device to the equivalent accepted argument
        gpu_device_hash = {
            'T4': 'nvidia-tesla-t4',
            'L4': 'nvidia-l4'
        }
        return {
            'CLUSTER_NAME': self.get_name(),
            'REGION': self.region,
            'ZONE': self.zone,
            'IMAGE': self.get_image_version(),
            'MASTER_MACHINE': self.get_master_node().instance_type,
            'WORKERS_COUNT': self.get_workers_count(),
            'WORKERS_MACHINE': worker_node.instance_type,
            'LOCAL_SSD': 2,
            'GPU_DEVICE': gpu_device_hash.get(gpu_device),
            'GPU_PER_WORKER': gpu_per_machine
        }


@dataclass
class DataprocSavingsEstimator(SavingsEstimator):
    """
    A class that calculates the savings based on Dataproc price provider
    """
    def __calculate_group_cost(self, cluster_inst: ClusterGetAccessor, node_type: SparkNodeType):
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
        master_cost = self.__calculate_group_cost(cluster, SparkNodeType.MASTER)
        workers_cost = self.__calculate_group_cost(cluster, SparkNodeType.WORKER)
        dataproc_cost = self.price_provider.get_container_cost()
        return master_cost + workers_cost + dataproc_cost

    def _setup_costs(self):
        # calculate target_cost
        self.target_cost = self._get_cost_per_cluster(self.reshaped_cluster)
        self.source_cost = self._get_cost_per_cluster(self.source_cluster)
