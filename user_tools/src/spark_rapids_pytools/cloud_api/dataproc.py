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

from dataclasses import dataclass, field
from typing import Any

from spark_rapids_pytools.cloud_api.gstorage import GStorageDriver
from spark_rapids_pytools.cloud_api.sp_types import PlatformBase, CMDDriverBase, CloudPlatform, ClusterBase, \
    ClusterNode, SysInfo, GpuHWInfo
from spark_rapids_pytools.common.utilities import SysCmd


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
                if self.ctxt.get(prop_entry.get(prop_entry_key)):
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
class DataprocCMDDriver(CMDDriverBase):
    """Represents the command interface that will be used by Dataproc"""

    def _list_inconsistent_configurations(self) -> list:
        incorrect_envs = super()._list_inconsistent_configurations()
        required_props = self.get_required_props()
        if required_props:
            for prop_entry in required_props:
                prop_value = self.env_vars.get(prop_entry)
                if prop_value is None:
                    incorrect_envs.append(f'Property {prop_value} is not set.')
        return incorrect_envs

    def _build_platform_list_cluster(self, cluster, query_args: dict = None) -> list:
        pass

    def pull_cluster_props_by_args(self, args: dict) -> str:
        cluster_name = args.get('cluster')
        # region is already set in the instance
        region_name = self.get_region()
        describe_cluster_cmd = ['gcloud',
                                'dataproc',
                                'clusters',
                                'describe',
                                cluster_name,
                                '--region',
                                region_name]
        return self.run_sys_cmd(describe_cluster_cmd)


@dataclass
class DataprocNode(ClusterNode):
    """Implementation of Dataproc cluster node."""

    def _pull_gpu_hw_info(self, cli=None) -> GpuHWInfo:
        pass

    def _pull_sys_info(self, cli=None) -> SysInfo:
        pass

    zone: str = field(default=None, init=False)


@dataclass
class DataprocCluster(ClusterBase):
    """
    Represents an instance of running cluster on Dataproc.
    """

    def get_eventlogs_from_config(self):
        pass
