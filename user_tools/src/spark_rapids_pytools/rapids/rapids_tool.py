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

"""Abstract class representing wrapper around the RAPIDS acceleration tools."""

import os
import re
import sys
import tarfile
from dataclasses import dataclass, field
from logging import Logger
from typing import Any, Callable, Dict

from spark_rapids_pytools.cloud_api.sp_types import CloudPlatform, get_platform, ClusterBase, DeployMode
from spark_rapids_pytools.common.sys_storage import FSUtil
from spark_rapids_pytools.common.utilities import ToolLogging, Utils
from spark_rapids_pytools.rapids.tool_ctxt import ToolContext


@dataclass
class RapidsTool(object):
    """
    A generic class that represents a RAPIDS plugin tool.
    :param platform_type: the type of platform associated with the current execution.
    :param cluster: name of the cluster on which the application will be running
    :param output_folder: location to store the output of the execution
    :param config_path: location of the configuration file of the current tool
    :param wrapper_options: dictionary containing options specific to the wrapper tool execution.
    :param rapids_options: dictionary containing the options to be passed as CLI arguments to the RAPIDS Accelerator.
    :param name: the name of the tool
    :param ctxt: context manager for the current tool execution.
    :param logger: the logger instant associated to the current tool.
    """
    platform_type: CloudPlatform
    cluster: str
    output_folder: str = None
    config_path: str = None
    runs_on_cluster: bool = field(default=True, init=False)
    wrapper_options: dict = field(default_factory=dict)
    rapids_options: dict = field(default_factory=dict)
    name: str = field(default=None, init=False)
    ctxt: ToolContext = field(default=None, init=False)
    logger: Logger = field(default=None, init=False)

    def pretty_name(self):
        return self.name.capitalize()

    def get_exec_cluster(self) -> ClusterBase:
        return self.ctxt.get_ctxt('execCluster')

    def phase_banner(phase_name: str,  # pylint: disable=no-self-argument
                     enable_prologue: bool = True,
                     enable_epilogue: bool = True):
        def decorator(func_cb: Callable):
            def wrapper(self, *args, **kwargs):
                try:
                    if enable_prologue:
                        self.logger.info('******* [%s]: Starting *******', phase_name)
                    func_cb(self, *args, **kwargs)  # pylint: disable=not-callable
                    if enable_epilogue:
                        self.logger.info('======= [%s]: Finished =======', phase_name)
                except Exception as exception:    # pylint: disable=broad-except
                    self.logger.error('%s. Phase [%s]: raised an error in phase\n%s',
                                      self.pretty_name(),
                                      phase_name,
                                      exception)
                    sys.exit(1)
            return wrapper
        return decorator

    def __post_init__(self):
        # when debug is set to true set it in the environment.
        self.logger = ToolLogging.get_and_setup_logger(f'rapids.tools.{self.name}')

    def _check_environment(self) -> None:
        self.ctxt.platform.setup_and_validate_env()

    def _process_output_args(self):
        self.logger.debug('Processing Output Arguments')
        # make sure that output_folder is being absolute
        if self.output_folder is None:
            self.output_folder = Utils.get_rapids_tools_env('OUTPUT_DIRECTORY', os.getcwd())
        self.output_folder = FSUtil.get_abs_path(self.output_folder)
        self.logger.debug('Root directory of local storage is set as: %s', self.output_folder)
        self.ctxt.set_local_workdir(self.output_folder)

    def _process_rapids_args(self):
        pass

    def _process_custom_args(self):
        pass

    def _process_job_submission_args(self):
        pass

    @phase_banner('Process-Arguments')
    def _process_arguments(self):
        # 0- process the output location
        self._process_output_args()
        # 1- process any arguments to be passed to the RAPIDS tool
        self._process_rapids_args()
        # 2- we need to process the arguments of the CLI
        self._process_custom_args()
        # 3- process submission arguments
        self._process_job_submission_args()

    @phase_banner('Initialization')
    def _init_tool(self):
        self._init_ctxt()
        self._check_environment()

    def _init_ctxt(self):
        if self.config_path is None:
            self.config_path = Utils.resource_path(f'{self.name}-conf.yaml')

        self.ctxt = ToolContext(platform_cls=get_platform(self.platform_type),
                                platform_opts=self.wrapper_options.get('platformOpts'),
                                prop_arg=self.config_path,
                                name=self.name)

    def _run_rapids_tool(self):
        # 1- copy dependencies to remote server
        # 2- prepare the arguments
        # 3- create a submission job
        # 4- execute
        pass

    @phase_banner('Execution')
    def _execute(self):
        """
        Phase representing actual execution of the wrapper command.
        """
        self._run_rapids_tool()

    def _process_output(self):
        pass

    def _delete_local_dep_folder(self):
        # clean_up the local dependency folder
        local_dep_folder = self.ctxt.get_local_work_dir()
        if self.ctxt.platform.storage.resource_exists(local_dep_folder):
            self.ctxt.platform.storage.remove_resource(local_dep_folder)

    def _delete_remote_dep_folder(self):
        # clean up the remote dep folder first
        remote_dep_folder = self.ctxt.get_remote('depFolder')
        if self.ctxt.platform.storage.resource_exists(remote_dep_folder):
            # delete the folder
            self.ctxt.platform.storage.remove_resource(remote_dep_folder)

    def _download_remote_output_folder(self):
        # download the output folder in to the local one with overriding
        remote_output_folder = self.ctxt.get_remote('workDir')
        local_folder = self.ctxt.get_local('outputFolder')
        self.ctxt.platform.storage.download_resource(remote_output_folder, local_folder)

    def _download_output(self):
        self._delete_local_dep_folder()
        # clean up the remote dep folder first
        self._delete_remote_dep_folder()
        # download the output folder in to the local one with overriding
        self._download_remote_output_folder()

    @phase_banner('Generating Report Summary',
                  enable_epilogue=False)
    def _finalize(self):
        print(Utils.gen_str_header(f'{self.pretty_name().upper()} Report',
                                   ruler='_',
                                   line_width=100))
        self._write_summary()

    def _write_summary(self):
        pass

    @phase_banner('Archiving Tool Output')
    def _archive_phase(self):
        self._archive_results()

    def _archive_results(self):
        pass

    @phase_banner('Collecting-Results')
    def _collect_result(self):
        """
        Following a successful run, collect and process data as needed
        :return:
        """
        self._download_output()
        self._process_output()

    @phase_banner('Connecting to Execution Cluster')
    def _connect_to_execution_cluster(self):
        """
        Connecting to execution cluster
        :return:
        """
        if self.runs_on_cluster:
            self.logger.info('%s requires the execution cluster %s to be running. '
                             'Establishing connection to cluster',
                             self.pretty_name(),
                             self.cluster)
            exec_cluster = self.ctxt.platform.connect_cluster_by_name(self.cluster)
            if not exec_cluster.is_cluster_running():
                self.logger.warning('Cluster %s is not running. Make sure that the execution cluster '
                                    'is in RUNNING state, then re-try.', exec_cluster.name)
            self.ctxt.set_ctxt('execCluster', exec_cluster)
        else:
            self.logger.info('%s requires no execution cluster. Skipping phase', self.pretty_name())

    def launch(self):
        self._init_tool()
        self._connect_to_execution_cluster()
        self._process_arguments()
        self._execute()
        self._collect_result()
        self._archive_phase()
        self._finalize()

    def _report_tool_full_location(self) -> str:
        pass

    def _report_results_are_empty(self):
        return [f'The {self.pretty_name()} tool did not generate any output. Nothing to display.']


@dataclass
class RapidsJarTool(RapidsTool):
    """
    A wrapper class to represent wrapper commands that require RAPIDS jar file.
    """
    runs_on_cluster = False

    def _process_jar_arg(self):
        tools_jar_url = self.wrapper_options.get('toolsJar')
        if tools_jar_url is None:
            tools_jar_url = self.ctxt.get_rapids_jar_url()
        # download the jar
        jar_path = self.ctxt.platform.storage.download_resource(tools_jar_url,
                                                                self.ctxt.get_local_work_dir(),
                                                                fail_ok=False,
                                                                create_dir=True)
        self.logger.info('RAPIDS accelerator jar is downloaded to work_dir %s', jar_path)
        # get the jar file name and add it to the tool args
        jar_file_name = FSUtil.get_resource_name(jar_path)
        self.ctxt.add_rapids_args('jarFileName', jar_file_name)
        self.ctxt.add_rapids_args('jarFilePath', jar_path)

    def __accept_tool_option(self, option_key: str) -> bool:
        defined_tool_options = self.ctxt.get_value_silent('sparkRapids', 'cli', 'tool_options')
        if defined_tool_options is not None:
            if option_key not in defined_tool_options:
                self.logger.warning('Ignoring tool option [%s]. Invalid option.', option_key)
                return False
        return True

    def _process_tool_args(self):
        """
        Process the arguments passed from the CLI if any and return a string representing the
        arguments to be passed to the final command running the job.
        :return:
        """
        arguments_list = []
        self.logger.debug('Processing Rapids plugin Arguments %s', self.rapids_options)
        raw_tool_opts: Dict[str, Any] = {}
        for key, value in self.rapids_options.items():
            if not isinstance(value, bool):
                # a boolean flag, does not need to have its value added to the list
                if isinstance(value, str):
                    # if the argument is multiple word, then protect it with single quotes.
                    if re.search(r'\s|\(|\)|,', value):
                        value = f"'{value}'"
                raw_tool_opts.setdefault(key, []).append(value)
            else:
                if value:
                    raw_tool_opts.setdefault(key, [])
                else:
                    # argument parser removes the "no-" prefix and set the value to false.
                    # we need to restore the original key
                    raw_tool_opts.setdefault(f'no{key}', [])
        for key, value in raw_tool_opts.items():
            self.logger.debug('Processing tool CLI argument.. %s:%s', key, value)
            if len(key) > 1:
                # python forces "_" to "-". we need to reverse that back.
                fixed_key = key.replace('_', '-')
                prefix = '--'
            else:
                # shortcut argument
                fixed_key = key
                prefix = '-'
            if self.__accept_tool_option(fixed_key):
                k_arg = f'{prefix}{fixed_key}'
                if len(value) >= 1:
                    # handle list options
                    for value_entry in value[0:]:
                        arguments_list.append(f'{k_arg}')
                        arguments_list.append(f'{value_entry}')
                else:
                    # this could be a boolean type flag that has no arguments
                    arguments_list.append(f'{k_arg}')
        self.ctxt.add_rapids_args('rapidsOpts', arguments_list)

    def _process_dependencies(self):
        """
        For local deployment mode, we need to process the extra dependencies specific to the platform
        :return:
        """
        if 'deployMode' in self.ctxt.platform_opts:
            # process the deployment
            # we need to download the dependencies locally if necessary
            self._download_dependencies()

    def _download_dependencies(self):
        # TODO: Verify the downloaded file by checking their MD5
        deploy_mode = DeployMode.tostring(self.ctxt.platform_opts.get('deployMode'))
        depend_arr = self.ctxt.platform.configs.get_value_silent('dependencies',
                                                                 'deployMode',
                                                                 deploy_mode)
        if depend_arr is not None:
            dest_folder = self.ctxt.get_cache_folder()
            dep_list = []
            for dep in depend_arr:
                self.logger.info('Checking dependency %s', dep['name'])
                resource_file_name = FSUtil.get_resource_name(dep['uri'])
                resource_file = FSUtil.build_path(dest_folder, resource_file_name)
                is_created = FSUtil.cache_from_url(dep['uri'], resource_file)
                if is_created:
                    self.logger.info('The dependency %s has been downloaded into %s', dep['uri'],
                                     resource_file)
                    # check if we need to decompress files
                if dep['type'] == 'archive':
                    destination_path = self.ctxt.get_local_work_dir()
                    with tarfile.open(resource_file, mode='r:*') as tar:
                        tar.extractall(destination_path)
                        tar.close()
                    dep_item = FSUtil.remove_ext(resource_file_name)
                    if dep.get('relativePath') is not None:
                        dep_item = FSUtil.build_path(dep_item, dep.get('relativePath'))
                    dep_item = FSUtil.build_path(destination_path, dep_item)
                else:
                    # copy the jar into dependency folder
                    dep_item = self.ctxt.platform.storage.download_resource(resource_file,
                                                                            self.ctxt.get_local_work_dir())
                dep_list.append(dep_item)
            self.logger.info('Dependencies are processed as: %s', ';'.join(dep_list))
            self.ctxt.add_rapids_args('javaDependencies', dep_list)

    def _process_rapids_args(self):
        # add a dictionary to hold the rapids arguments
        self._process_jar_arg()
        self._process_dependencies()
        self._process_tool_args()
