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

"""Implementation specific to DATABRICKS_AWS"""

from dataclasses import dataclass, field
from typing import Any

from spark_rapids_pytools.cloud_api.databricks_aws_job import DBAWSLocalRapidsJob
from spark_rapids_pytools.cloud_api.emr import EMRNode, EMRPlatform
from spark_rapids_pytools.cloud_api.s3storage import S3StorageDriver
from spark_rapids_pytools.cloud_api.sp_types import CloudPlatform, CMDDriverBase, ClusterBase, ClusterNode, ClusterState, SparkNodeType
from spark_rapids_pytools.common.prop_manager import JSONPropertiesContainer
from spark_rapids_pytools.pricing.databricks_pricing import DatabricksPriceProvider
from spark_rapids_pytools.pricing.price_provider import SavingsEstimator


@dataclass
class DBAWSPlatform(EMRPlatform):
    """
    Represents the interface and utilities required by DATABRICKS_AWS.
    Prerequisites:
    - install databricks, aws command lines (databricks cli, aws cli)
    - configure the databricks cli (token, workspace, profile)
    - configure the aws cli
    """
    def __post_init__(self):
        self.type_id = CloudPlatform.DATABRICKS_AWS
        super(EMRPlatform, self).__post_init__()

    def _construct_cli_object(self) -> CMDDriverBase:
        return DBAWSCMDDriver(timeout=0, cloud_ctxt=self.ctxt)

    def _install_storage_driver(self):
        self.storage = S3StorageDriver(super().cli)

    def _construct_cluster_from_props(self, cluster: str, props: str = None):
        return DatabricksCluster(self).set_connection(cluster_id=cluster, props=props)

    def set_offline_cluster(self, cluster_args: dict = None):
        pass

    def migrate_cluster_to_gpu(self, orig_cluster):
        """
        given a cluster, convert it to run NVIDIA Gpu based on mapping instance types
        :param orig_cluster: the original cluster to migrate from
        :return: a new object cluster that supports GPU
        """
        gpu_cluster_ob = DatabricksCluster(self)
        gpu_cluster_ob.migrate_from_cluster(orig_cluster)
        return gpu_cluster_ob

    def create_saving_estimator(self, source_cluster, target_cluster):
        raw_pricing_config = self.configs.get_value_silent('pricing')
        if raw_pricing_config:
            pricing_config = JSONPropertiesContainer(prop_arg=raw_pricing_config, file_load=False)
        else:
            pricing_config: JSONPropertiesContainer = None
        databricks_price_provider = DatabricksPriceProvider(region=self.cli.get_region(),
                                                            pricing_configs={'databricks': pricing_config})
        saving_estimator = DBAWSSavingsEstimator(price_provider=databricks_price_provider,
                                                 target_cluster=target_cluster,
                                                 source_cluster=source_cluster)
        return saving_estimator

    def create_submission_job(self, job_prop, ctxt) -> Any:
        pass

    def create_local_submission_job(self, job_prop, ctxt) -> Any:
        return DBAWSLocalRapidsJob(prop_container=job_prop, exec_ctxt=ctxt)

    def validate_job_submission_args(self, submission_args: dict) -> dict:
        pass


@dataclass
class DBAWSCMDDriver(CMDDriverBase):
    """Represents the command interface that will be used by DATABRICKS_AWS"""

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

    def _build_platform_describe_node_instance(self, node: ClusterNode) -> list:
        cmd_params = ['aws ec2 describe-instance-types',
                      '--region', f'{self.get_region()}',
                      '--instance-types', f'{node.instance_type}']
        return cmd_params


@dataclass
class DatabricksNode(EMRNode):
    """Implementation of Databricks cluster node."""

    region: str = field(default=None, init=False)

    def _set_fields_from_props(self):
        self.name = self.props.get_value('public_dns')


@dataclass
class DatabricksCluster(ClusterBase):
    """
    Represents an instance of running cluster on Databricks.
    """

    def get_eventlogs_from_config(self) -> list:
        res_arr = []
        eventlogs_dir = self.props.get_value_silent('spark_conf', 'spark.eventLog.dir')
        if eventlogs_dir:
            res_arr.append(eventlogs_dir)
        return res_arr

    def _set_fields_from_props(self):
        super()._set_fields_from_props()
        self.uuid = self.props.get_value('cluster_id')
        self.state = ClusterState.fromstring(self.props.get_value('state'))

    def _init_nodes(self):
        # assume that only one master node
        master_nodes_from_conf = self.props.get_value('driver')
        worker_nodes_from_conf = self.props.get_value('executors')
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
            worker = DatabricksNode.create_worker_node().set_fields_from_dict(worker_props)
            worker.fetch_and_set_hw_info(self.cli)
            worker_nodes.append(worker)
        master_props = {
            'Id': master_nodes_from_conf['node_id'],
            'props': JSONPropertiesContainer(prop_arg=master_nodes_from_conf, file_load=False),
            # set the node region based on the wrapper defined region
            'region': self.region,
            'instance_type': self.props.get_value('driver_node_type_id')
        }
        master_node = DatabricksNode.create_master_node().set_fields_from_dict(master_props)
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
            new_node = DatabricksNode.create_worker_node().set_fields_from_dict(worker_props)
            new_worker_nodes.append(new_node)
        self.nodes = {
            SparkNodeType.WORKER: new_worker_nodes,
            SparkNodeType.MASTER: orig_cluster.nodes.get(SparkNodeType.MASTER)
        }
        if bool(mc_type_map):
            # update the platform notes
            self.platform.update_ctxt_notes('nodeConversions', mc_type_map)


@dataclass
class DBAWSSavingsEstimator(SavingsEstimator):
    """
    A class that calculates the savings based on a Databricks-AWS price provider
    """

    def __calculate_ec2_cost(self, cluster: DatabricksCluster):
        master_instance = cluster.nodes.get(SparkNodeType.MASTER)
        worker_instances = cluster.nodes.get(SparkNodeType.WORKER)
        master_ec2_cost = self.price_provider.catalogs['aws'].get_value('ec2', master_instance.instance_type)
        if len(worker_instances) == 0:
            return master_ec2_cost
        workers_ec2_unit_cost = self.price_provider.catalogs['aws'].get_value('ec2', worker_instances[0].instance_type)
        workers_ec2_cost = workers_ec2_unit_cost * len(worker_instances)
        return master_ec2_cost + workers_ec2_cost

    def _get_cost_per_cluster(self, cluster: DatabricksCluster):
        master_instance = cluster.nodes.get(SparkNodeType.MASTER)
        master_cost = self.price_provider.get_instance_price(instance=master_instance.instance_type)
        worker_instances = cluster.nodes.get(SparkNodeType.WORKER)
        workers_cost = 0.0
        if len(worker_instances) != 0:
            worker_unit_cost = self.price_provider.get_instance_price(instance=worker_instances[0].instance_type)
            workers_cost = worker_unit_cost * len(worker_instances)
        return self.__calculate_ec2_cost(cluster) + master_cost + workers_cost

    def _setup_costs(self):
        # calculate target_cost
        self.target_cost = self._get_cost_per_cluster(self.target_cluster)
        self.source_cost = self._get_cost_per_cluster(self.source_cluster)
