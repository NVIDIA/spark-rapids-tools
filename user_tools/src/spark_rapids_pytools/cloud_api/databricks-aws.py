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
        super().__post_init__()
    
    def _set_remaining_configuration_list(self) -> None:
        remaining_props = self._get_config_environment('loadedConfigProps')
        if not remaining_props:
            return
        properties_map_arr = self._get_config_environment('cliConfig',
                                                          'confProperties',
                                                          'propertiesMap')
        # TODO: did not finish implementation
    
    def _construct_cli_object(self) -> CMDDriverBase:
        return DBAWSCMDDriver(timeout=0, cloud_ctxt=self.ctxt)
    
    def _construct_cluster_from_props(self, cluster: str, props: str = None):
        return DatabricksCluster(self).set_connection(cluster_id=cluster, props=props)
    
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
class DBAWSCMDDriver(CMDDriverBase):
    """Represents the command interface that will be used by DATABRICKS_AWS"""

    def _list_inconsistent_configurations(self) -> list:
        incorrect_envs = super()._list_inconsistent_configurations()
        required_props = self.get_required_props()
        if required_props is not None:
            for prop_entry in required_props:
                prop_value = self.env_vars.get(prop_entry)
                if prop_value is None:
                    incorrect_envs.append(f'Property {prop_value} is not set.')
        return incorrect_envs

    def _build_platform_list_cluster(self, cluster, query_args: dict = None) -> list:
        pass

    def pull_cluster_props_by_args(self, args: dict) -> str:
        db_cluster_id = args.get('Id')
        if db_cluster_id is not None:
            get_cluster_cmd = ['databricks',
                               'clusters',
                               'get',
                               '--cluster-id',
                               db_cluster_id]
            return self.run_sys_cmd(get_cluster_cmd)
        db_cluster_name = args.get('cluster')
        if db_cluster_name is not None:
            get_cluster_cmd = ['databricks',
                               'clusters',
                               'get',
                               '--cluster-name',
                               db_cluster_name]
            return self.run_sys_cmd(get_cluster_cmd)
        error_msg = f'Could not find Databricks cluster by Id or by name'
        raise RuntimeError(error_msg)

@dataclass
class DatabricksCluster(ClusterBase):
    """
    Represents an instance of running cluster on Databricks.
    """

    def get_eventlogs_from_config(self):
        pass