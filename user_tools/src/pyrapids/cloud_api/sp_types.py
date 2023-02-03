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

"""Service providers defined types"""
import json
from dataclasses import dataclass, field
from enum import Enum
from logging import Logger
from shutil import which
from typing import cast, Type, Any

from pyrapids.common.prop_manager import AbstractPropertiesContainer, JSONPropertiesContainer
from pyrapids.common.sys_storage import StorageDriver
from pyrapids.common.utilities import ToolLogging, resource_path, SysCmd


class EnumeratedType(str, Enum):
    """Abstract representation of enumerated values"""

    @classmethod
    def tostring(cls, value):
        # type: (Union[Enum, str]) -> str
        """Return the string representation of the state object attribute
        :param str value: the state object to turn into string
        :return: the uppercase string that represents the state object
        :rtype: str
        """
        value = cast(Enum, value)
        return str(value._value_).upper()  # pylint: disable=protected-access

    @classmethod
    def fromstring(cls, value):
        # type: (str) -> Optional[str]
        """Return the state object attribute that matches the string
        :param str value: the string to look up
        :return: the state object attribute that matches the string
        :rtype: str
        """
        return getattr(cls, value.upper(), None)

    @classmethod
    def pretty_print(cls, value):
        # type: (Union[Enum, str]) -> str
        """Return the string representation of the state object attribute
        :param str value: the state object to turn into string
        :return: the string that represents the state object
        :rtype: str
        """
        value = cast(Enum, value)
        return str(value._value_)  # pylint: disable=protected-access


class DeployMode(EnumeratedType):
    """List of tools deployment methods"""
    SERVERLESS = 'serverless'
    LOCAL = 'local'


class GpuDevice(EnumeratedType):
    """List of supported GPU devices"""
    T4 = 't4'
    V100 = 'v100'
    K80 = 'k80'
    A100 = 'a100'
    P100 = 'P100'

    @classmethod
    def get_default_gpu(cls):
        return cls.T4


class ClusterState(EnumeratedType):
    """
    Standard states for a cluster.
    """
    STARTING = 'starting'
    BOOTSTRAPPING = 'bootstrapping'
    WAITING = 'waiting'
    RUNNING = 'running'
    TERMINATING = 'terminating'
    TERMINATED = 'terminated'
    TERMINATED_WITH_ERRORS = 'terminated_with_errors'
    STOPPED = 'stopped'
    OFFLINE = 'offline'
    UNKNOWN = 'unknown'


class CloudPlatform(EnumeratedType):
    """symbolic names (members) bound to supported cloud platforms."""
    DATAPROC = 'dataproc'
    EMR = 'emr'
    LOCAL = 'local'
    NONE = 'NONE'


class SparkNodeType(EnumeratedType):
    """
    Node type from Spark perspective. We either have a master node or a worker node.
    Note that the provider could have different grouping.
    For example EMR has: master, task, and core.
    Another categorization: onDemand..etc.
    """
    MASTER = 'master'
    WORKER = 'worker'


@dataclass
class SysInfo:
    num_cpus: int = None
    cpu_mem: int = None


@dataclass
class GpuHWInfo:
    num_gpus: int = None
    gpu_mem: int = None
    gpu_device: GpuDevice = GpuDevice.get_default_gpu()


@dataclass
class NodeHWInfo:
    sys_info: SysInfo = None
    gpu_info: GpuHWInfo = None

    def is_gpu_node(self) -> bool:
        return self.gpu_info is not None


@dataclass
class ClusterNode:
    """
    Represents a single cluster node.
    :param node_type: type from Spark perspective (Worker vs Master)
    :param name: name of the node used to remote access in SSH
    :param instance_type: the instance type running on the node
    :param mc_props: holds the properties of the instance type running on the node.
                    This is used for further processing.
    :param hw_info: contains hardware settings of the node: System and GPU.
    """
    node_type: SparkNodeType
    name: str = field(default=None, init=False)
    instance_type: str = field(default=None, init=False)
    props: AbstractPropertiesContainer = field(default=None, init=False)
    mc_props: AbstractPropertiesContainer = field(default=None, init=False)
    hw_info: NodeHWInfo = field(default=None, init=False)

    def set_fields_from_dict(self, field_values: dict = None):
        if field_values is not None:
            for field_name in field_values:
                setattr(self, field_name, field_values.get(field_name))
        self._set_fields_from_props()
        return self

    def _set_fields_from_props(self):
        pass

    def _pull_and_set_mc_props(self, cli=None):
        pass

    def _pull_gpu_hw_info(self, cli=None) -> GpuHWInfo:
        raise NotImplementedError

    def _pull_sys_info(self, cli=None) -> SysInfo:
        raise NotImplementedError

    def _construct_hw_info(self,
                           cli=None,
                           gpu_info: GpuHWInfo = None,
                           sys_info: SysInfo = None):
        del cli  # Unused cli, defined for future use.
        self.hw_info = NodeHWInfo(sys_info=sys_info,
                                  gpu_info=gpu_info)

    def fetch_and_set_hw_info(self, cli=None):
        self._pull_and_set_mc_props(cli)
        try:
            # if a node has no gpu, then it is expected that setting the gpu info fails
            gpu_info = self._pull_gpu_hw_info(cli)
        except Exception as ex:  # pylint: disable=broad-except
            cli.logger.info(f'Could not pull GPU info for '
                            f'{SparkNodeType.tostring(self.node_type)} node {self.name}: {ex}')
            gpu_info = None
        sys_info = self._pull_sys_info(cli)
        self._construct_hw_info(cli=cli, gpu_info=gpu_info, sys_info=sys_info)

    def find_best_cpu_conversion(self, target_list: dict):
        target_cpus = self.hw_info.sys_info.num_cpus
        best_match = None
        last_record = None
        for prof_id, hw_info in target_list.items():
            # assume that the list is sorted by number of CPUs
            last_record = prof_id
            if hw_info.sys_info.num_cpus <= target_cpus:
                best_match = prof_id
        if best_match is None:
            best_match = last_record
        return best_match

    @classmethod
    def create_worker_node(cls) -> Any:
        return cls(SparkNodeType.WORKER)

    @classmethod
    def create_master_node(cls) -> Any:
        return cls(SparkNodeType.MASTER)

    @classmethod
    def create_node(cls, value):
        if isinstance(value, SparkNodeType):
            if value == SparkNodeType.MASTER:
                return cls.create_master_node()
            if value == SparkNodeType.WORKER:
                return cls.create_worker_node()
        raise RuntimeError(f'Invalid node type while creating cluster node {value}')


@dataclass
class CMDDriverBase:
    """
    Represents the command interface that will be used by the platform
    It has:
    1- the command used by the platform. for example gcloud, gsutil, or AWS
    2- the ssh command to nodes (it could include authentications)
    3- normal commands
    :param cloud_ctxt: dictionary containing all the necessary configurations related to the CSP.
    :param timeout: How long to wait (in seconds) for the command to finish (optional).
    :param: system_prerequisites: list of the tools required to run the given instance.
    :param: env_vars: dictionary containing all the variables required by the driver.
    """
    cloud_ctxt: dict
    timeout: int = 0
    system_prerequisites: list = field(default_factory=lambda: [])
    env_vars: dict = field(default_factory=dict, init=False)
    logger: Logger = field(default=ToolLogging.get_and_setup_logger('rapids.tools.cmd'), init=False)

    @classmethod
    def is_system_tool(cls, tool_name):
        """
        check whether a tool is installed on the system.
        :param tool_name: name of the tool to check
        :return: True or False
        """
        return which(tool_name) is not None

    def get_env_var(self, key: str):
        return self.env_vars.get(key)

    def get_region(self) -> str:
        return self.env_vars.get('region')

    def get_and_set_env_vars(self):
        """For that driver, try to get all the available system environment for the system."""
        for env_key in ['profile', 'region', 'aws_access_id', 'aws_access_key']:
            if env_key in self.cloud_ctxt:
                self.env_vars.update({env_key: self.cloud_ctxt.get(env_key)})

    def validate_env(self):
        incorrect_envs = []
        for sys_tool in self.system_prerequisites:
            if not self.is_system_tool(sys_tool):
                incorrect_envs.append(f'Tool {sys_tool} is not in installed or not in the PATH environment')
        if self.get_region() is None:
            incorrect_envs.append('Platform region is not set.')
        if len(incorrect_envs) > 0:
            exc_msg = '; '.join(incorrect_envs)
            raise RuntimeError(f'Invalid environment: {exc_msg}')

    def run_sys_cmd(self,
                    cmd,
                    cmd_input: str = None,
                    fail_ok: bool = False,
                    env_vars: dict = None) -> str:
        def process_streams(std_out, std_err):
            if ToolLogging.is_debug_mode_enabled():
                # reformat lines to make the log more readable
                stdout_splits = std_out.splitlines()
                stderr_splits = std_err.splitlines()
                stdout_str = ''
                if len(stdout_splits) > 0:
                    std_out_lines = '\n'.join([f'\t| {line}' for line in stdout_splits])
                    stdout_str = f'\n\t<STDOUT>\n{std_out_lines}'
                if len(stderr_splits) > 0:
                    std_err_lines = '\n'.join([f'\t| {line}' for line in stderr_splits])
                    stderr_str = f'\n\t<STDERR>\n{std_err_lines}'
                    self.logger.debug('executing CMD:\n\t<CMD: %s>[%s]; [%s]', cmd, stdout_str, stderr_str)
        cmd_args = {
            'cmd': cmd,
            'fail_ok': fail_ok,
            'cmd_input': cmd_input,
            'env_vars': env_vars,
            'process_streams_cb': process_streams
        }
        sys_cmd = SysCmd().build(cmd_args)
        return sys_cmd.exec()

    def _build_ssh_cmd_prefix_for_node(self, node: ClusterNode) -> str:
        del node  # Unused by super method.
        return ''

    def _construct_ssh_cmd_with_prefix(self, prefix: str, remote_cmd: str) -> str:
        return f'{prefix} {remote_cmd}'

    def ssh_cmd_node(self, node: ClusterNode, ssh_cmd: str, cmd_input: str = None) -> str:
        prefix_cmd = self._build_ssh_cmd_prefix_for_node(node=node)
        full_ssh_cmd = self._construct_ssh_cmd_with_prefix(prefix=prefix_cmd, remote_cmd=ssh_cmd)
        return self.run_sys_cmd(full_ssh_cmd, cmd_input=cmd_input)

    def pull_cluster_props_by_args(self, args: dict) -> str or None:
        del args  # Unused by super method.
        return ''

    def _build_platform_describe_node_instance(self, node: ClusterNode) -> list:
        del node  # Unused by super method.
        return []

    def exec_platform_describe_node_instance(self, node: ClusterNode) -> str:
        """
        Given a node, execute platform CLI to pull the properties of the instance type running on
        that node.
        :param node: node object
        :return: string containing the properties of the machine. The string could be in json or yaml format.
        """
        cmd_params = self._build_platform_describe_node_instance(node=node)
        return self.run_sys_cmd(cmd_params)

    def _build_platform_list_cluster(self,
                                     cluster,
                                     query_args: dict = None) -> list:
        raise NotImplementedError

    def exec_platform_list_cluster_instances(self,
                                             cluster,
                                             query_args: dict = None) -> str:
        cmd_args = self._build_platform_list_cluster(cluster=cluster, query_args=query_args)
        return self.run_sys_cmd(cmd_args)


@dataclass
class PlatformBase:
    """
    Represents the common methods used by all other platforms.
    We need to load constants about platform:
    1- supported machine types
    2- supported Images or non-supported images
    3- GPU specs
    4- pricing catalog
    """
    ctxt_args: dict
    type_id: CloudPlatform = field(default_factory=lambda: CloudPlatform.NONE, init=False)
    cli: CMDDriverBase = field(default=None, init=False)
    storage: StorageDriver = field(default=None, init=False)
    ctxt: dict = field(default_factory=dict, init=False)
    configs: JSONPropertiesContainer = field(default=None, init=False)

    @classmethod
    def list_supported_gpus(cls):
        return [GpuDevice.T4, GpuDevice.A100]

    def _create_cli_instance(self) -> CMDDriverBase:
        raise NotImplementedError

    def _install_storage_driver(self):
        raise NotImplementedError

    def _parse_arguments(self, ctxt_args: dict):
        if 'region' in ctxt_args:
            region_val = ctxt_args.get('region')
            if region_val is not None:
                # add the region to the ctxt
                self.ctxt.update({'region': region_val})

    def __post_init__(self):
        self.load_platform_configs()
        self.ctxt = {'platformType': self.type_id}
        self.ctxt = {'notes': {}}
        self._parse_arguments(self.ctxt_args)
        self.cli = self._create_cli_instance()
        self._install_storage_driver()

    def update_ctxt_notes(self, note_key, note_value):
        self.ctxt['notes'].update({note_key: note_value})

    def setup_and_validate_env(self):
        if self.cli is not None:
            self.cli.get_and_set_env_vars()
            self.cli.validate_env()
        # TODO we should fail if the CLI is None

    def _construct_cluster_from_props(self,
                                      cluster: str,
                                      props: str = None):
        raise NotImplementedError

    def set_offline_cluster(self, cluster_args: dict = None):
        raise NotImplementedError

    def load_cluster_by_prop_file(self, cluster_prop_path: str):
        prop_container = JSONPropertiesContainer(prop_arg=cluster_prop_path)
        return self._construct_cluster_from_props(cluster=None,
                                                  props=json.dumps(prop_container.props))

    def connect_cluster_by_name(self, cluster: str):
        """
        To be used to if the cluster can be found in the platform.
        :param cluster:
        :return:
        """
        cluster_props = self.cli.pull_cluster_props_by_args(args={'cluster': cluster})
        return self._construct_cluster_from_props(cluster=cluster,
                                                  props=cluster_props)

    def migrate_cluster_to_gpu(self, orig_cluster):
        """
        given a cluster, convert it to run NVIDIA Gpu based on mapping instance types
        :param orig_cluster: the cluster on which the application was executed and is running
                without acceleration.
        :return: a new object cluster that supports GPU
        """
        raise NotImplementedError

    def create_saving_estimator(self, source_cluster, target_cluster):
        raise NotImplementedError

    def create_submission_job(self, job_prop, ctxt) -> Any:
        raise NotImplementedError

    def create_local_submission_job(self, job_prop, ctxt) -> Any:
        raise NotImplementedError

    def load_platform_configs(self):
        config_file_name = f'{CloudPlatform.tostring(self.type_id).lower()}-configs.json'
        config_path = resource_path(config_file_name)
        self.configs = JSONPropertiesContainer(prop_arg=config_path)

    def get_supported_gpus(self) -> dict:
        gpus_from_configs = self.configs.get_value('gpuConfigs', 'user-tools', 'supportedGpuInstances')
        gpu_scopes = {}
        for mc_prof, mc_info in gpus_from_configs.items():
            hw_info_json = mc_info['SysInfo']
            hw_info_ob = SysInfo(num_cpus=hw_info_json['num_cpus'], cpu_mem=hw_info_json['cpu_mem'])
            gpu_info_json = mc_info['GpuHWInfo']
            gpu_info_obj = GpuHWInfo(num_gpus=gpu_info_json['num_gpus'], gpu_mem=gpu_info_json['gpu_mem'])
            gpu_scopes[mc_prof] = NodeHWInfo(sys_info=hw_info_ob, gpu_info=gpu_info_obj)
        return gpu_scopes

    def validate_job_submission_args(self, submission_args: dict) -> dict:
        raise NotImplementedError


@dataclass
class ClusterBase:
    """
    Represents an instance of a cluster on the platform.
    Cluster can be running/offline
    """
    platform: PlatformBase
    cli: CMDDriverBase = field(default=None, init=False)
    name: str = field(default=None, init=False)
    uuid: str = field(default=None, init=False)
    region: str = field(default=None, init=False)
    zone: str = field(default=None, init=False)
    state: ClusterState = field(default=ClusterState.RUNNING, init=False)
    nodes: dict = field(default_factory=dict, init=False)
    props: AbstractPropertiesContainer = field(default=None, init=False)

    def __post_init__(self):
        self.cli = self.platform.cli
        self.region = self.cli.get_region()

    def _init_connection(self, cluster_id: str = None,
                         props: str = None) -> dict:
        del props, cluster_id  # Unused by super method.
        return {}

    def set_fields_from_dict(self, field_values: dict = None):
        """
        Given a dictionary, this function is to set the fields of the cluster.
        :param field_values: the dictionary containing the key/value pair to initialize the cluster.
        :return:
        """
        if field_values is not None:
            for field_name in field_values:
                setattr(self, field_name, field_values.get(field_name))
        self._set_fields_from_props()

    def _process_loaded_props(self) -> None:
        """
        After loading the raw properties, perform any necessary processing to clean up the
        properties.
        """
        return None

    def _set_fields_from_props(self):
        self._process_loaded_props()

    def _init_nodes(self):
        pass

    def set_connection(self,
                       cluster_id: str = None,
                       props: str = None):
        """
        Setting a connection to an existing Connection to a cluster, then we need to pull properties.
        :param cluster_id: the argument to be used to fetch the cluster.
        :param props: optional argument that includes dictionary of the platform cluster's description.
        :return: a cluster
        """
        pre_init_args = self._init_connection(cluster_id, props)
        self.set_fields_from_dict(pre_init_args)
        self._init_nodes()
        return self

    def is_cluster_running(self) -> bool:
        return self.state == ClusterState.RUNNING

    def get_eventlogs_from_config(self):
        raise NotImplementedError

    def run_cmd_driver(self, ssh_cmd: str, cmd_input: str = None) -> str or None:
        """
        Execute command on the driver node
        :param ssh_cmd: the command to be executed on the remote node. Note that the quotes
                        surrounding the shell command should be included.
        :param cmd_input: optional argument string used as an input to the command line.
                        i.e., writing to a file.
        :return:
        """
        # get the master node
        master_node: ClusterNode = self.get_master_node()
        return self.cli.ssh_cmd_node(master_node, ssh_cmd, cmd_input=cmd_input)

    def run_cmd_worker(self, ssh_cmd: str, cmd_input: str = None, ind: int = 0) -> str or None:
        """
        Execute command on the worker node
        :param ssh_cmd: the command to be executed on the remote node. Note that the quotes
                        surrounding the shell command should be included.
        :param cmd_input: optional argument string used as an input to the command line.
                          i.e., writing to a file.
        :param ind: the node index. By default, the command is executed on first worker node.
        """
        # get the worker node
        worker_node: ClusterNode = self.get_worker_node(ind)
        return self.cli.ssh_cmd_node(worker_node, ssh_cmd, cmd_input=cmd_input)

    def get_worker_hw_info(self) -> NodeHWInfo:
        worker_node = self.get_worker_node()
        return worker_node.hw_info

    def get_master_node(self) -> ClusterNode:
        return self.nodes.get(SparkNodeType.MASTER)

    def get_worker_node(self, ind: int = 0) -> ClusterNode:
        return self.nodes.get(SparkNodeType.WORKER)[ind]


def get_platform(platform_id: Enum) -> Type[PlatformBase]:
    platform_hash = {
        # Dataproc platform is not enabled yet
        # CloudPlatform.DATAPROC: ('pyrapids.cloud_api.dataproc', 'DataprocPlatform'),
        CloudPlatform.EMR: ('pyrapids.cloud_api.emr', 'EMRPlatform'),
    }
    if platform_id in platform_hash:
        mod_name, clz_name = platform_hash[platform_id]
        imported_mod = __import__(mod_name, globals(), locals(), [clz_name])
        return getattr(imported_mod, clz_name)
    raise AttributeError(f'Provider {platform_id} does not exist')
