# Copyright (c) 2023-2025, NVIDIA CORPORATION.
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

"""Implementation specific to EMR"""

import json
import os
from dataclasses import field, dataclass
from typing import Any, List

from spark_rapids_tools import CspEnv
from spark_rapids_pytools.cloud_api.emr_job import EmrLocalRapidsJob
from spark_rapids_pytools.cloud_api.s3storage import S3StorageDriver
from spark_rapids_pytools.cloud_api.sp_types import PlatformBase, ClusterBase, CMDDriverBase, \
    ClusterState, SparkNodeType, ClusterNode, GpuHWInfo, GpuDevice, ClusterGetAccessor
from spark_rapids_pytools.common.prop_manager import JSONPropertiesContainer, \
    AbstractPropertiesContainer
from spark_rapids_pytools.common.utilities import Utils
from spark_rapids_pytools.pricing.emr_pricing import EMREc2PriceProvider
from spark_rapids_pytools.pricing.price_provider import SavingsEstimator


@dataclass
class EMRPlatform(PlatformBase):
    """
    Represents the interface and utilities required by AWS EMR.
    Prerequisites:
    - install aws command lines (aws cli)
    - configure the aws
        - this may be done by region
    - aws has no staging available in the cluster properties.
    - gsutil is used to move data from/to storage
    """

    def set_offline_cluster(self, cluster_args: dict = None):
        pass

    @classmethod
    def get_spark_node_type_fromstring(cls, value) -> SparkNodeType:
        if value.upper() in ['TASK', 'CORE']:
            return SparkNodeType.WORKER
        return SparkNodeType.fromstring(value)

    @classmethod
    def process_raw_cluster_prop(cls, prop_container: AbstractPropertiesContainer) -> str:
        if prop_container.get_value_silent('Cluster'):
            _, prop_container.props = prop_container.props.popitem()
        return json.dumps(prop_container.props)

    def __post_init__(self):
        self.type_id = CspEnv.EMR
        self.cluster_inference_supported = True
        super().__post_init__()

    def _construct_cli_object(self) -> CMDDriverBase:
        return EMRCMDDriver(timeout=0, cloud_ctxt=self.ctxt)

    def _install_storage_driver(self):
        self.storage = S3StorageDriver(self.cli)

    def _construct_cluster_from_props(self, cluster: str, props: str = None, is_inferred: bool = False,
                                      is_props_file: bool = False):
        return EMRCluster(self, is_inferred=is_inferred, is_props_file=is_props_file).\
            set_connection(cluster_id=cluster, props=props)

    def migrate_cluster_to_gpu(self, orig_cluster):
        """
        given a cluster, convert it to run NVIDIA Gpu based on mapping instance types
        :param orig_cluster: the original cluster to migrate from
        :return: a new object cluster that supports GPU
        """
        gpu_cluster_ob = EMRCluster(self)
        gpu_cluster_ob.migrate_from_cluster(orig_cluster)
        return gpu_cluster_ob

    def validate_job_submission_args(self, submission_args: dict) -> dict:
        """
        process the job submission and return the final arguments to be used for the execution.
        :param submission_args: dictionary containing the job submission arguments
        :return: a dictionary with the processed arguments.
        """
        # TODO: verify that all arguments are valid
        return submission_args

    def create_saving_estimator(self,
                                source_cluster: ClusterGetAccessor,
                                reshaped_cluster: ClusterGetAccessor,
                                target_cost: float = None,
                                source_cost: float = None):
        raw_pricing_config = self.configs.get_value_silent('pricing')
        if raw_pricing_config:
            pricing_config = JSONPropertiesContainer(prop_arg=raw_pricing_config, file_load=False)
        else:
            pricing_config: JSONPropertiesContainer = None
        emr_price_provider = EMREc2PriceProvider(region=self.cli.get_region(),
                                                 pricing_configs={'emr': pricing_config})
        saving_estimator = EmrSavingsEstimator(price_provider=emr_price_provider,
                                               reshaped_cluster=reshaped_cluster,
                                               source_cluster=source_cluster,
                                               target_cost=target_cost,
                                               source_cost=source_cost)
        return saving_estimator

    def create_local_submission_job(self, job_prop, ctxt) -> Any:
        return EmrLocalRapidsJob(prop_container=job_prop, exec_ctxt=ctxt)

    def create_distributed_submission_job(self, job_prop, ctxt) -> Any:
        pass

    def generate_cluster_configuration(self, render_args: dict):
        image_version = self.configs.get_value_silent('clusterInference', 'defaultImage')
        render_args['IMAGE'] = f'"{image_version}"'
        render_args['ZONE'] = f'"{self.cli.get_zone()}"'
        return super().generate_cluster_configuration(render_args)


@dataclass
class EMRCMDDriver(CMDDriverBase):
    """Represents the command interface that will be used by EMR"""

    def _list_inconsistent_configurations(self) -> list:
        incorrect_envs = super()._list_inconsistent_configurations()
        # check that private key file path is correct
        emr_pem_path = self.env_vars.get('keyPairPath')
        if emr_pem_path is not None:
            if not os.path.exists(emr_pem_path):
                incorrect_envs.append(f'Private key file path [{emr_pem_path}] does not exist. '
                                      'It is required to SSH on driver node.')
            else:
                # check valid extension
                if not (emr_pem_path.endswith('.pem') or emr_pem_path.endswith('ppk')):
                    incorrect_envs.append(f'Private key file path [{emr_pem_path}] should be ppk or pem format')
        else:
            tools_env_k = Utils.find_full_rapids_tools_env_key('KEY_PAIR_PATH')
            incorrect_envs.append(
                f'Private key file path is not set. It is required to SSH on driver node. '
                f'Set {tools_env_k}')
        return incorrect_envs

    def pull_cluster_props_by_args(self, args: dict) -> str:
        aws_cluster_id = args.get('Id')
        cluster_name = args.get('cluster')
        if aws_cluster_id is None:
            # use cluster name to get the cluster values
            # we need to get the cluster_id from the list command first.
            list_cmd_res = self.exec_platform_list_cluster_by_name(cluster_name)
            error_msg = f'Could not find EMR cluster {cluster_name} by name'
            if not list_cmd_res:
                raise RuntimeError(error_msg)
            # listed_cluster is json formatted string of array, but we need only the first entry
            # to read the clusterID
            cluster_headers: list = json.loads(list_cmd_res)
            if len(cluster_headers) == 0:
                raise RuntimeError(error_msg)
            existing_cluster = cluster_headers[0]
            aws_cluster_id = existing_cluster['Id']
        self.logger.debug('Cluster %s has an Id %s', cluster_name, aws_cluster_id)
        cluster_described = self.exec_platform_describe_cluster_by_id(aws_cluster_id)
        if cluster_described is not None:
            raw_prop_container = JSONPropertiesContainer(prop_arg=cluster_described, file_load=False)
            return EMRPlatform.process_raw_cluster_prop(raw_prop_container)
        return cluster_described

    def _build_cmd_ssh_prefix_for_node(self, node: ClusterNode) -> str:
        # get the pem file
        pem_file_path = self.env_vars.get('keyPairPath')
        prefix_args = ['ssh',
                       '-o StrictHostKeyChecking=no',
                       f'-i {pem_file_path}',
                       f'hadoop@{node.name}']
        return Utils.gen_joined_str(' ', prefix_args)

    def _build_cmd_scp_to_node(self, node: ClusterNode, src: str, dest: str) -> str:
        # get the pem file
        pem_file_path = self.env_vars.get('keyPairPath')
        prefix_args = ['scp',
                       '-o StrictHostKeyChecking=no',
                       f'-i {pem_file_path}',
                       src,
                       f'hadoop@{node.name}:{dest}']
        return Utils.gen_joined_str(' ', prefix_args)

    def _build_cmd_scp_from_node(self, node: ClusterNode, src: str, dest: str) -> str:
        # get the pem file
        pem_file_path = self.env_vars.get('keyPairPath')
        prefix_args = ['scp',
                       '-o StrictHostKeyChecking=no',
                       f'-i {pem_file_path}',
                       f'hadoop@{node.name}:{src}',
                       dest]
        return Utils.gen_joined_str(' ', prefix_args)

    def get_zone(self) -> str:
        describe_cmd = ['aws ec2 describe-availability-zones',
                        '--region', f'{self.get_region()}']
        selected_zone = ''
        try:
            zones_list = json.loads(self.run_sys_cmd(describe_cmd))
            selected_zone = zones_list['AvailabilityZones'][0]['ZoneName']
        except Exception:  # pylint: disable=broad-except
            self.logger.warning('Unable to extract zone from region %s', self.get_region())
        return selected_zone

    def _build_platform_list_cluster(self,
                                     cluster,
                                     query_args: dict = None) -> list:
        # aws emr list-instances --cluster-id j-2DDF0Q87QOXON
        cmd_params = ['aws emr list-instances',
                      '--cluster-id',
                      f'{cluster.uuid}']
        if query_args is not None:
            for q_key in query_args:
                cmd_params.append(f'--{q_key}')
                cmd_params.append(f'{query_args.get(q_key)}')
        return cmd_params

    def exec_platform_list_cluster_by_name(self,
                                           cluster_name: str):
        list_cmd = f"aws emr list-clusters --query 'Clusters[?Name==`{cluster_name}`]'"
        return self.run_sys_cmd(list_cmd)

    def exec_platform_describe_cluster_by_id(self,
                                             cluster_id: str):
        describe_cmd = f'aws emr describe-cluster --cluster-id {cluster_id}'
        return self.run_sys_cmd(describe_cmd)

    def get_submit_spark_job_cmd_for_cluster(self, cluster_name: str, submit_args: dict) -> List[str]:
        raise NotImplementedError

    def _process_instance_description(self, instance_descriptions: str) -> dict:
        processed_instance_descriptions = {}
        raw_instances_descriptions = JSONPropertiesContainer(prop_arg=instance_descriptions, file_load=False)
        for instance in raw_instances_descriptions.get_value('InstanceTypes'):
            instance_content = {}
            instance_content['VCpuCount'] = int(instance.get('VCpuInfo', {}).get('DefaultVCpus', -1))
            instance_content['MemoryInMB'] = int(instance.get('MemoryInfo', {}).get('SizeInMiB', -1))
            if 'GpuInfo' in instance:
                gpu_name = instance['GpuInfo']['Gpus'][0]['Name']
                gpu_count = int(instance['GpuInfo']['Gpus'][0]['Count'])
                instance_content['GpuInfo'] = [{'Name': gpu_name, 'Count': [gpu_count]}]
            processed_instance_descriptions[instance.get('InstanceType')] = instance_content
        return processed_instance_descriptions

    def get_instance_description_cli_params(self):
        return ['aws ec2 describe-instance-types', '--region', f'{self.get_region()}']


@dataclass
class InstanceGroup:
    """
    Holds information about instance groups
    """
    id: str  # group ID
    instance_type: str  # the machine type
    count: int  # Number of requested instances associated to that group
    market: str  # ON_DEMAND OR ON_SPOT
    group_type: str  # Master, TASK, or CORE
    spark_grp_type: SparkNodeType = field(default=None, init=False)  # map the group_type to Spark type.
    state: ClusterState  # RUNNING, TERMINATED..etc.

    def __post_init__(self):
        self.spark_grp_type = EMRPlatform.get_spark_node_type_fromstring(self.group_type)


@dataclass
class Ec2Instance:
    """
    Holds information about instance groups
    """
    id: str
    ec2_instance_id: str
    dns_name: str
    group: InstanceGroup
    state: ClusterState  # RUNNING, TERMINATED..etc.


@dataclass
class EMRNode(ClusterNode):
    """
    Represents EMR cluster Node.
    We assume that all nodes are running on EC2 instances.
    """
    ec2_instance: Ec2Instance = field(default=None, init=False)

    def _set_fields_from_props(self):
        self.name = self.ec2_instance.dns_name
        self.instance_type = self.ec2_instance.group.instance_type

    def _pull_gpu_hw_info(self, cli=None) -> GpuHWInfo or None:
        raw_gpus = self.mc_props.get_value_silent('GpuInfo')
        if raw_gpus is None or len(raw_gpus) == 0:
            return None
        # TODO: we assume all gpus of the same type
        raw_gpu = raw_gpus[0]
        gpu_device = GpuDevice.fromstring(raw_gpu['Name'])
        gpu_cnt = raw_gpu['Count'][0]  # gpu count is a list
        gpu_mem = GpuDevice.get_gpu_mem(gpu_device)[0]  # gpu memory is a list, picking the first one
        return GpuHWInfo(num_gpus=gpu_cnt,
                         gpu_device=gpu_device,
                         gpu_mem=gpu_mem)


@dataclass
class EMRCluster(ClusterBase):
    """
    Represents an instance of running cluster on EMR.
    """
    instance_groups: list = field(default=None, init=False)
    ec2_instances: list = field(default=None, init=False)

    def _process_loaded_props(self) -> None:
        """
        After loading the raw properties, perform any necessary processing to clean up the
        properties. We want to get rid of Cluster
        """
        if self.props.get_value_silent('Cluster') is not None:
            _, new_props = self.props.props.popitem()
            self.props.props = new_props

    def __create_ec2_list_by_group(self, group_arg):
        if isinstance(group_arg, InstanceGroup):
            group_obj = group_arg
            group_id = group_arg.id
        else:
            group_id = group_arg
            group_obj = None
        render_args = {
            'INSTANCE_GROUP_ID': f'"{group_id}"',
            'INSTANCE_TYPE': f'"{group_obj.instance_type}"'
        }
        if self.is_inferred:
            # If the instance is inferred, generate a default list of node configurations
            instances_list = self.generate_node_configurations(group_obj.count, render_args)
        else:
            query_args = {'instance-group-id': group_id}
            try:
                # Fetch cluster instances based on instance group id
                raw_instance_list = self.cli.exec_platform_list_cluster_instances(self, query_args=query_args)
                instances_list = json.loads(raw_instance_list).get('Instances')
            except Exception:  # pylint: disable=broad-except
                # If instance list creation fails, generate a default list of node configurations
                self.logger.error('Failed to create configurations for %s instances in group %s. '
                                  'Using generated names.', group_obj.count, group_id)
                instances_list = self.generate_node_configurations(group_obj.count, render_args)
        ec2_instances = []
        for raw_inst in instances_list:
            parsed_state = raw_inst['Status']['State']
            ec2_instance = Ec2Instance(
                id=raw_inst['Id'],
                ec2_instance_id=raw_inst['Ec2InstanceId'],
                dns_name=raw_inst['PublicDnsName'],
                group=group_obj,
                state=ClusterState.fromstring(parsed_state)
            )
            ec2_instances.append(ec2_instance)
        return ec2_instances

    def _build_migrated_cluster(self, orig_cluster):
        """
        specific to the platform on how to build a cluster based on migration
        :param orig_cluster:
        """
        group_cache = {}
        self.instance_groups = []
        self.ec2_instances = []
        # get the map of the instance types
        mc_type_map = orig_cluster.find_matches_for_node()
        # convert instances and groups
        # master groups should stay the same
        for curr_group in orig_cluster.instance_groups:
            if curr_group.spark_grp_type == SparkNodeType.MASTER:
                new_inst_grp = curr_group
            else:
                # convert the instance_type
                new_instance_type = mc_type_map.get(curr_group.instance_type, curr_group.instance_type)
                if new_instance_type == curr_group.instance_type:
                    new_inst_grp = curr_group
                else:
                    new_inst_grp = InstanceGroup(
                        id=curr_group.id,
                        instance_type=new_instance_type,
                        count=curr_group.count,
                        market=curr_group.market,
                        group_type=curr_group.group_type,
                        state=ClusterState.get_default())
                group_cache.update({new_inst_grp.id: new_inst_grp})
            self.instance_groups.append(new_inst_grp)
        # convert the instances
        for ec2_inst in orig_cluster.ec2_instances:
            if ec2_inst.group.spark_grp_type == SparkNodeType.MASTER:
                new_group_obj = ec2_inst.group
            else:
                # get the new group
                new_group_obj = group_cache.get(ec2_inst.group.id)
            new_inst = Ec2Instance(
                id=ec2_inst.id,
                ec2_instance_id=ec2_inst.ec2_instance_id,
                dns_name=None,
                group=new_group_obj,
                state=ec2_inst.state)
            self.ec2_instances.append(new_inst)
        self.nodes = self.__create_node_from_instances()
        if bool(mc_type_map):
            # update the platform notes
            self.platform.update_ctxt_notes('nodeConversions', mc_type_map)

    def __create_node_from_instances(self):
        worker_nodes = []
        master_nodes = []
        for ec2_inst in self.ec2_instances:
            node_props = {
                'ec2_instance': ec2_inst
            }
            c_node = EMRNode.create_node(ec2_inst.group.spark_grp_type).set_fields_from_dict(node_props)
            c_node.fetch_and_set_hw_info(self.cli)
            if c_node.node_type == SparkNodeType.WORKER:
                worker_nodes.append(c_node)
            else:
                master_nodes.append(c_node)
        return {
            SparkNodeType.WORKER: worker_nodes,
            SparkNodeType.MASTER: master_nodes[0]
        }

    def _init_nodes(self):
        def process_cluster_group_list(inst_groups: list) -> list:
            instance_group_list = []
            for inst_grp in inst_groups:
                try:
                    parsed_state = ClusterState.fromstring(inst_grp['Status']['State'])
                except (KeyError, ValueError):
                    parsed_state = ClusterState.get_default()
                    self.logger.info(f'Unable to get cluster state, setting to '
                                     f'\'{ClusterState.tostring(parsed_state)}\'.')
                inst_group = InstanceGroup(
                    id=inst_grp['Id'],
                    instance_type=inst_grp['InstanceType'],
                    count=inst_grp['RequestedInstanceCount'],
                    market=inst_grp['Market'],
                    group_type=inst_grp['InstanceGroupType'],
                    state=parsed_state
                )
                instance_group_list.append(inst_group)
            return instance_group_list

        # get instance_groups from the cluster props.
        inst_grps = self.props.get_value('InstanceGroups')
        self.instance_groups = process_cluster_group_list(inst_grps)
        self.ec2_instances = []
        for curr_group in self.instance_groups:
            if self.is_props_file:
                for _ in range(curr_group.count):
                    ec2_instance = Ec2Instance(
                        id='',
                        ec2_instance_id='',
                        dns_name='',
                        group=curr_group,
                        state=curr_group.state
                    )
                    self.ec2_instances.append(ec2_instance)
            else:
                instances_list = self.__create_ec2_list_by_group(curr_group)
                self.ec2_instances.extend(instances_list)
        self.nodes = self.__create_node_from_instances()

    def _set_fields_from_props(self):
        super()._set_fields_from_props()
        self.uuid = self.props.get_value('Id')
        self.state = ClusterState.fromstring(self.props.get_value('Status', 'State'))
        self.zone = self.props.get_value('Ec2InstanceAttributes',
                                         'Ec2AvailabilityZone')

    def _set_name_from_props(self) -> None:
        self.name = self.props.get_value('Name')

    def is_cluster_running(self) -> bool:
        acceptable_init_states = [
            ClusterState.RUNNING,
            ClusterState.STARTING,
            ClusterState.BOOTSTRAPPING,
            ClusterState.WAITING
        ]
        return self.state in acceptable_init_states

    def get_all_spark_properties(self) -> dict:
        res = {}
        configs_list = self.props.get_value_silent('Configurations')
        for conf_item in configs_list:
            if conf_item['Classification'].startswith('spark'):
                curr_spark_props = conf_item['Properties']
                res.update(curr_spark_props)
        return res

    def get_tmp_storage(self) -> str:
        raise NotImplementedError

    def get_image_version(self) -> str:
        return self.props.get_value('ReleaseLabel')

    def _set_render_args_create_template(self) -> dict:
        render_args = super()._set_render_args_create_template()
        render_args['IMAGE'] = self.get_image_version()
        return render_args


@dataclass
class EmrSavingsEstimator(SavingsEstimator):
    """
    A class that calculates the savings based on an EMR price provider
    """

    def _calculate_ec2_cost(self,
                            cluster_inst: ClusterGetAccessor,
                            node_type: SparkNodeType) -> float:
        nodes_cnt = cluster_inst.get_nodes_cnt(node_type)
        node_mc_type = cluster_inst.get_node_instance_type(node_type)
        ec2_unit_cost = self.price_provider.catalogs['aws'].get_value('ec2', node_mc_type)
        ec2_cost = ec2_unit_cost * nodes_cnt
        return ec2_cost

    def _calculate_emr_cost(self,
                            cluster_inst: ClusterGetAccessor,
                            node_type: SparkNodeType) -> float:
        nodes_cnt = cluster_inst.get_nodes_cnt(node_type)
        node_mc_type = cluster_inst.get_node_instance_type(node_type)
        emr_unit_cost = self.price_provider.catalogs['aws'].get_value('emr', node_mc_type)
        emr_cost = emr_unit_cost * nodes_cnt
        return emr_cost

    def _get_cost_per_cluster(self, cluster: ClusterGetAccessor):
        total_cost = 0.0
        for node_type in [SparkNodeType.MASTER, SparkNodeType.WORKER]:
            total_cost += self._calculate_ec2_cost(cluster, node_type)
            total_cost += self._calculate_emr_cost(cluster, node_type)
        return total_cost
