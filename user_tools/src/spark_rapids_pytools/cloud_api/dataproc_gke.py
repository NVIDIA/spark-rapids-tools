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


"""Implementation specific to Dataproc"""

from dataclasses import dataclass, field
from typing import Any

from spark_rapids_pytools.cloud_api.dataproc import DataprocCluster, DataprocCMDDriver, DataprocNode, \
    DataprocPlatform, DataprocSavingsEstimator
from spark_rapids_pytools.cloud_api.dataproc_gke_job import DataprocGkeLocalRapidsJob
from spark_rapids_pytools.cloud_api.sp_types import CMDDriverBase, \
    SparkNodeType, ClusterGetAccessor
from spark_rapids_pytools.common.prop_manager import JSONPropertiesContainer
from spark_rapids_pytools.common.sys_storage import FSUtil
from spark_rapids_pytools.pricing.dataproc_gke_pricing import DataprocGkePriceProvider
from spark_rapids_tools import CspEnv


@dataclass
class DataprocGkePlatform(DataprocPlatform):
    """
    Represents the interface and utilities required by DataprocGke.
    Prerequisites:
    - install gcloud command lines (gcloud, gsutil)
    - configure the gcloud CLI.
    - dataproc_gke has staging temporary storage. we can retrieve that from the cluster properties.
    """

    def __post_init__(self):
        super().__post_init__()
        self.type_id = CspEnv.DATAPROC_GKE
        self.cluster_inference_supported = False

    @classmethod
    def get_spark_node_type_fromstring(cls, value: str):
        node_type_mapping = {
            'SPARK_EXECUTOR': SparkNodeType.WORKER,
            'SPARK_DRIVER': SparkNodeType.MASTER,
        }
        return node_type_mapping.get(value.upper())

    def _construct_cli_object(self) -> CMDDriverBase:
        return DataprocGkeCMDDriver(timeout=0, cloud_ctxt=self.ctxt)

    def _construct_cluster_from_props(self, cluster: str, props: str = None, is_inferred: bool = False,
                                      is_props_file: bool = False):
        return DataprocGkeCluster(self, is_inferred=is_inferred).set_connection(cluster_id=cluster, props=props)

    def migrate_cluster_to_gpu(self, orig_cluster):
        """
        given a cluster, convert it to run NVIDIA Gpu based on mapping instance types
        :param orig_cluster: the original cluster to migrate from
        :return: a new object cluster that supports GPU.
        """
        gpu_cluster_ob = DataprocGkeCluster(self)
        gpu_cluster_ob.migrate_from_cluster(orig_cluster)
        return gpu_cluster_ob

    def create_saving_estimator(self,
                                source_cluster: ClusterGetAccessor,
                                reshaped_cluster: ClusterGetAccessor,
                                target_cost: float = None,
                                source_cost: float = None):
        raw_pricing_config = self.configs.get_value_silent('pricing')
        if raw_pricing_config:
            pricing_config = JSONPropertiesContainer(prop_arg=raw_pricing_config,
                                                     file_load=False)
        else:
            pricing_config: JSONPropertiesContainer = None
        pricing_provider = DataprocGkePriceProvider(region=self.cli.get_region(),
                                                    pricing_configs={'gcloud': pricing_config})
        saving_estimator = DataprocGkeSavingsEstimator(price_provider=pricing_provider,
                                                       reshaped_cluster=reshaped_cluster,
                                                       source_cluster=source_cluster,
                                                       target_cost=target_cost,
                                                       source_cost=source_cost)
        return saving_estimator

    def create_local_submission_job(self, job_prop, ctxt) -> Any:
        return DataprocGkeLocalRapidsJob(prop_container=job_prop, exec_ctxt=ctxt)

    def create_distributed_submission_job(self, job_prop, ctxt) -> Any:
        pass


@dataclass
class DataprocGkeCMDDriver(DataprocCMDDriver):
    """Represents the command interface that will be used by DataprocGke"""

    def pull_node_pool_props_by_args(self, args: dict) -> str:
        node_pool_name = args.get('node_pool_name')
        gke_cluster_name = args.get('gke_cluster_name')
        if 'region' in args:
            region_name = args.get('region')
        else:
            region_name = self.get_region()
        describe_cluster_cmd = ['gcloud',
                                'container',
                                'node-pools',
                                'describe',
                                node_pool_name,
                                '--cluster',
                                gke_cluster_name,
                                '--region',
                                region_name]
        return self.run_sys_cmd(describe_cluster_cmd)


@dataclass
class GkeNodePool:
    """
    Holds information about node pools
    """
    name: str
    roles: list
    gke_cluster_name: str
    spark_node_type: SparkNodeType = field(default=None, init=False)  # map the role to Spark type.

    def __post_init__(self):
        # only the first role is considered
        if len(self.roles) > 0:
            self.spark_node_type = DataprocGkePlatform.get_spark_node_type_fromstring(self.roles[0])


@dataclass
class DataprocGkeCluster(DataprocCluster):
    """
    Represents an instance of running cluster on DataprocGke.
    """
    node_pools: list = field(default=None, init=False)

    @staticmethod
    def __extract_info_from_value(conf_val: str):
        if '/' in conf_val:
            # this is a valid url-path
            return FSUtil.get_resource_name(conf_val)
        # this is a value
        return conf_val

    def _init_node_pools(self):
        gke_cluster_config = self.props.get_value('virtualClusterConfig', 'kubernetesClusterConfig', 'gkeClusterConfig')
        gke_cluster_name = self.__extract_info_from_value(gke_cluster_config['gkeClusterTarget'])
        raw_node_pools = gke_cluster_config['nodePoolTarget']

        def create_node_pool_elem(node_pool: dict) -> GkeNodePool:
            pool_name = self.__extract_info_from_value(node_pool['nodePool'])
            return GkeNodePool(name=pool_name, roles=node_pool['roles'], gke_cluster_name=gke_cluster_name)

        self.node_pools = [create_node_pool_elem(node_pool) for node_pool in raw_node_pools]

    def _init_nodes(self):
        self._init_node_pools()

        def create_cluster_node(node_pool):
            if node_pool.spark_node_type is None:
                return None
            args = {'node_pool_name': node_pool.name, 'gke_cluster_name': node_pool.gke_cluster_name}
            raw_node_props = self.cli.pull_node_pool_props_by_args(args)
            node_props = JSONPropertiesContainer(prop_arg=raw_node_props, file_load=False)
            node = DataprocNode.create_node(node_pool.spark_node_type).set_fields_from_dict({
                'name': node_props.get_value('name'),
                'props': JSONPropertiesContainer(prop_arg=node_props.get_value('config'), file_load=False),
                'zone': self.zone
            })
            node.fetch_and_set_hw_info(self.cli)
            return node

        executor_nodes = []
        driver_nodes = []
        for gke_node_pool in self.node_pools:
            c_node = create_cluster_node(gke_node_pool)
            if gke_node_pool.spark_node_type == SparkNodeType.WORKER:
                executor_nodes.append(c_node)
            elif gke_node_pool.spark_node_type == SparkNodeType.MASTER:
                driver_nodes.append(c_node)
        self.nodes = {
            SparkNodeType.WORKER: executor_nodes,
            SparkNodeType.MASTER: driver_nodes[0]
        }


@dataclass
class DataprocGkeSavingsEstimator(DataprocSavingsEstimator):
    """
    A class that calculates the savings based on DataprocGke price provider
    """

    def _get_cost_per_cluster(self, cluster: ClusterGetAccessor):
        dataproc_cost = super()._get_cost_per_cluster(cluster)
        dataproc_gke_cost = self.price_provider.get_container_cost()
        return dataproc_cost + dataproc_gke_cost
