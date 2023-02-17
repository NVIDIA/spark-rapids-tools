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

import configparser
import json
from dataclasses import dataclass, field
from enum import Enum
from logging import Logger
from typing import cast, Type, Any

from spark_rapids_pytools.common.prop_manager import AbstractPropertiesContainer, JSONPropertiesContainer
from spark_rapids_pytools.common.sys_storage import StorageDriver, FSUtil
from spark_rapids_pytools.common.utilities import ToolLogging, SysCmd, Utils


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
    :param: env_vars: dictionary containing all the variables required by the driver.
    """
    cloud_ctxt: dict
    timeout: int = 0
    env_vars: dict = field(default_factory=dict, init=False)
    logger: Logger = field(default=ToolLogging.get_and_setup_logger('rapids.tools.cmd'), init=False)

    def get_env_var(self, key: str):
        return self.env_vars.get(key)

    def get_region(self) -> str:
        return self.env_vars.get('region')

    def get_cmd_run_configs(self) -> str:
        return self.env_vars.get('cmdRunnerProperties')

    def get_required_props(self) -> list:
        cmd_runner_props = self.get_cmd_run_configs()
        if cmd_runner_props:
            return cmd_runner_props.get('inheritedProps')
        return None

    def get_system_prerequisites(self) -> list:
        res = []
        cmd_runner_props = self.get_cmd_run_configs()
        if cmd_runner_props:
            res.extend(cmd_runner_props.get('systemPrerequisites'))
        return res

    def get_piggyback_props(self) -> list:
        res = []
        cmd_runner_props = self.get_cmd_run_configs()
        if cmd_runner_props:
            return cmd_runner_props.get('cliPiggyBackEnvVars')['definedVars']
        return res

    def get_and_set_env_vars(self):
        """For that driver, try to get all the available system environment for the system."""
        for item_key in self.cloud_ctxt:
            # save all not-None entries to the env_vars
            item_value = self.cloud_ctxt.get(item_key)
            if item_value is not None:
                self.env_vars[item_key] = item_value

    def _list_inconsistent_configurations(self) -> list:
        """
        List all the inconsistent configuration in the platform
        :return: a list of inconsistencies
        """
        incorrect_envs = []
        for sys_tool in self.get_system_prerequisites():
            if not Utils.is_system_tool(sys_tool):
                incorrect_envs.append(f'Tool {sys_tool} is not in installed or not in the PATH environment')
        if self.get_region() is None:
            incorrect_envs.append('Platform region is not set.')
        return incorrect_envs

    def _handle_inconsistent_configurations(self, incorrect_envs: list) -> None:
        if len(incorrect_envs) > 0:
            # we do not want to raise a runtime error because some of the flags are not required by
            # all the tools.
            # TODO: improve this by checking the requirements for each tool.
            exc_msg = '; '.join(incorrect_envs)
            self.logger.warning('Environment report: %s', exc_msg)

    def validate_env(self):
        incorrect_envs = self._list_inconsistent_configurations()
        self._handle_inconsistent_configurations(incorrect_envs)

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
                if isinstance(cmd, str):
                    cmd_log_str = cmd
                else:
                    cmd_log_str = ' '.join(cmd)
                if len(stderr_splits) > 0:
                    std_err_lines = '\n'.join([f'\t| {line}' for line in stderr_splits])
                    stderr_str = f'\n\t<STDERR>\n{std_err_lines}'
                    self.logger.debug('executing CMD:\n\t<CMD: %s>[%s]; [%s]',
                                      cmd_log_str,
                                      stdout_str,
                                      stderr_str)
        piggyback_vars = self.get_piggyback_props()
        for var_entry in piggyback_vars:
            prop_label = var_entry['confProperty']
            var_value = self.get_env_var(prop_label)
            if var_value:
                if not env_vars:
                    env_vars = {var_entry['varKey']: var_value}
                else:
                    # use setdefault in case the env_car was already defined
                    env_vars.setdefault(var_entry['varKey'], var_value)
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
    logger: Logger = field(default=ToolLogging.get_and_setup_logger('rapids.tools.csp'), init=False)

    @classmethod
    def list_supported_gpus(cls):
        return [GpuDevice.T4, GpuDevice.A100]

    def load_from_config_parser(self, conf_file, **prop_args) -> dict:
        res = None
        try:
            parser_obj = configparser.ConfigParser()
            parser_obj.read(conf_file)
            res = {}
            if prop_args.get('sectionKey'):
                section_name = prop_args.get('sectionKey')
                if not parser_obj.has_section(section_name):
                    # try to use "profile XYZ" format
                    if parser_obj.has_section(f'profile {section_name}'):
                        section_name = f'profile {section_name}'
                key_list = prop_args.get('keyList')
                for k in key_list:
                    if parser_obj.has_option(section_name, k):
                        res.update({k: parser_obj.get(section_name, k)})
        except (configparser.NoSectionError, configparser.NoOptionError, configparser.ParsingError) as conf_ex:
            self.logger.debug('Could not load properties from configuration file %s. Exception: %s',
                              conf_file, conf_ex)
        return res

    def _construct_cli_object(self) -> CMDDriverBase:
        raise NotImplementedError

    def _create_cli_instance(self) -> CMDDriverBase:
        cmd_driver_props = self._get_config_environment('cmdRunnerProperties')
        self.ctxt['cmdRunnerProperties'] = cmd_driver_props
        return self._construct_cli_object()

    def _install_storage_driver(self):
        raise NotImplementedError

    def _get_config_environment(self, *key_strs) -> Any:
        return self.configs.get_value('environment', *key_strs)

    def _load_config_environment_var_prop(self, prop_key: str):
        env_variables = self._get_config_environment('cliConfig', 'envVariables')
        # find the env_variable that maps to the property
        res = []
        for env_var in env_variables:
            if env_var['confProperty'] == prop_key:
                res.append(env_var)
        return res

    def _set_env_prop_from_env_var(self, prop_key: str) -> None:
        if self.ctxt.get(prop_key):
            # it is already set. do nothing
            return
        # find the env_variable that maps to the property
        for env_var in self._load_config_environment_var_prop(prop_key):
            env_var_key = env_var['envVariableKey']
            env_var_def_val = env_var.get('defaultValue')
            env_var_val = Utils.get_sys_env_var(env_var_key, env_var_def_val)
            if env_var_val is not None:
                if '/' in env_var_val:
                    # this is a file
                    env_var_val = FSUtil.expand_path(env_var_val)
                self.ctxt.update({prop_key: env_var_val})
                break

    def _set_initial_configuration_list(self) -> None:
        # load the initial configurations list
        initial_conf_list = self._get_config_environment('initialConfigList')
        if initial_conf_list:
            for conf_prop_k in initial_conf_list:
                self._set_env_prop_from_env_var(conf_prop_k)

    def _set_remaining_configuration_list(self) -> None:
        remaining_props = self._get_config_environment('loadedConfigProps')
        if not remaining_props:
            return
        properties_map_arr = self._get_config_environment('cliConfig',
                                                          'confProperties',
                                                          'propertiesMap')
        if properties_map_arr:
            prop_keys = []
            for prop_elem in properties_map_arr:
                if prop_elem.get('confProperty') in remaining_props:
                    prop_keys.append(prop_elem.get('propKey'))
            # TODO: we should check if the section is of pattern _PropName_,
            #       then we extract that property and use it as  the section name
            loaded_conf_dict = self._load_props_from_sdk_conf_file(keyList=prop_keys,
                                                                   sectionKey=self.ctxt.get('profile'))
            if loaded_conf_dict:
                self.ctxt.update(loaded_conf_dict)
            for prop_elem in properties_map_arr:
                if prop_elem.get('propKey') not in loaded_conf_dict:
                    # set it using environment variable if possible
                    self._set_env_prop_from_env_var(prop_elem.get('propKey'))

    def _set_credential_properties(self) -> None:
        cli_credential_file = self.ctxt.get('credentialFile')
        if cli_credential_file is None:
            return
        properties_map_arr = self._get_config_environment('cliConfig',
                                                          'confProperties',
                                                          'credentialsMap')
        if not properties_map_arr:
            return
        prop_keys = []
        for prop_elem in properties_map_arr:
            prop_keys.append(prop_elem.get('propKey'))
        # TODO: we should check if the section is of pattern _PropName_,
        #       then we extract that property and use it as  the section name
        loaded_conf_dict = self.load_from_config_parser(cli_credential_file,
                                                        keyList=prop_keys,
                                                        sectionKey=self.ctxt.get('profile'))
        if loaded_conf_dict:
            self.ctxt.update(loaded_conf_dict)

    def _parse_arguments(self, ctxt_args: dict):
        # Get the possible parameters for that platform.
        # Arguments passed to the tool have more precedence than global env variables
        list_of_params = self._get_config_environment('envParams')
        if list_of_params:
            for param_elem in list_of_params:
                param_val = ctxt_args.get(param_elem)
                if param_val:
                    # add the argument to the context
                    self.ctxt.update({param_elem: param_val})
        self._set_initial_configuration_list()
        # load the remaining properties
        self._set_remaining_configuration_list()
        # load the credential properties
        self._set_credential_properties()

    def _load_props_from_sdk_conf_file(self, **prop_args) -> dict:
        cli_conf_file = self.ctxt.get('cliConfigFile')
        if cli_conf_file is None:
            return None
        return self.load_from_config_parser(cli_conf_file, **prop_args)

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
        config_path = Utils.resource_path(config_file_name)
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
        CloudPlatform.DATAPROC: ('spark_rapids_pytools.cloud_api.dataproc', 'DataprocPlatform'),
        CloudPlatform.EMR: ('spark_rapids_pytools.cloud_api.emr', 'EMRPlatform'),
    }
    if platform_id in platform_hash:
        mod_name, clz_name = platform_hash[platform_id]
        imported_mod = __import__(mod_name, globals(), locals(), [clz_name])
        return getattr(imported_mod, clz_name)
    raise AttributeError(f'Provider {platform_id} does not exist')
