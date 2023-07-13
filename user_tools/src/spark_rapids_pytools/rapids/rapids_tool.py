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

import concurrent
import logging
import os
import re
import sys
import tarfile
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from logging import Logger
from typing import Any, Callable, Dict, List

from spark_rapids_pytools.cloud_api.sp_types import CloudPlatform, get_platform, \
    ClusterBase, DeployMode
from spark_rapids_pytools.common.sys_storage import FSUtil
from spark_rapids_pytools.common.utilities import ToolLogging, Utils
from spark_rapids_pytools.rapids.rapids_job import RapidsJobPropContainer
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
    cluster: str = None
    output_folder: str = None
    config_path: str = None
    wrapper_options: dict = field(default_factory=dict)
    rapids_options: dict = field(default_factory=dict)
    name: str = field(default=None, init=False)
    ctxt: ToolContext = field(default=None, init=False)
    logger: Logger = field(default=None, init=False)

    def pretty_name(self):
        return self.name.capitalize()

    def get_exec_cluster(self) -> ClusterBase:
        return self.ctxt.get_ctxt('execCluster')

    def is_remote_cluster_execution(self) -> bool:
        """
        used to verify whether a rapids tool runs on a remote cluster submission.
        This does not include the serverlessMode
        :return: True when the tool needs to have a remote cluster established
        """
        return self.ctxt.get_deploy_mode() == DeployMode.REMOTE_CLUSTER

    def requires_remote_folder(self) -> bool:
        """
        used to verify whether a rapids tool running remotely has a defined remote path to generate the
        rapids tool output.
        :return: True when the tool needs to have a remote cluster folder
        """
        deply_mode: DeployMode = self.ctxt.get_deploy_mode()
        return deply_mode.requires_remote_storage()

    def requires_cluster_connection(self) -> bool:
        return self.is_remote_cluster_execution()

    def timeit(timed_item: str):  # pylint: disable=no-self-argument
        def decorator(func_cb: Callable):
            def wrapper(self, *args, **kwargs):
                start_time = time.monotonic()
                func_cb(self, *args, **kwargs)  # pylint: disable=not-callable
                end_time = time.monotonic()
                self.logger.info('Total Execution Time: %s => %s seconds', timed_item,
                                 f'{(end_time-start_time):,.3f}')
            return wrapper
        return decorator

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
                except Exception:    # pylint: disable=broad-except
                    logging.exception('%s. Raised an error in phase [%s]\n',
                                      self.pretty_name(),
                                      phase_name)
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
            # delete the folder. Note that for dataproc this will also delete the parent directory when it is empty
            self.ctxt.platform.storage.remove_resource(remote_dep_folder)

    def _download_remote_output_folder(self):
        # download the output folder in to the local one with overriding
        remote_output_folder = self.ctxt.get_remote('workDir')
        # for dataproc it is possible that the entire directory has been deleted when it is empty
        if self.ctxt.platform.storage.resource_exists(remote_output_folder):
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
        if self.requires_cluster_connection():
            self.logger.info('%s requires the execution cluster %s to be running. '
                             'Establishing connection to cluster',
                             self.pretty_name(),
                             self.cluster)
            exec_cluster = self.ctxt.platform.connect_cluster_by_name(self.cluster)
            self.ctxt.set_ctxt('execCluster', exec_cluster)
            self._verify_exec_cluster()
        else:
            self.logger.info('%s requires no execution cluster. Skipping phase', self.pretty_name())

    def _handle_non_running_exec_cluster(self, err_msg: str) -> None:
        self.logger.warning(err_msg)

    def _verify_exec_cluster(self):
        # For remote job we should fail once we find that the cluster is not actually running
        exec_cluster = self.get_exec_cluster()
        if exec_cluster and exec_cluster.is_cluster_running():
            return
        # For remote cluster mode, the execution cluster must be running
        if not exec_cluster:
            msg = 'An error initializing the execution cluster'
        else:
            msg = f'Remote execution Cluster [{exec_cluster.get_name()}] is not active. ' \
                  f'The execution cluster should be in RUNNING state'
        self._handle_non_running_exec_cluster(msg)

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

    def _generate_section_lines(self, sec_conf: dict) -> List[str]:
        all_lines = sec_conf['content'].get('lines')
        if all_lines:
            return all_lines
        return None

    def _generate_section_content(self, sec_conf: dict) -> List[str]:
        sec_title = sec_conf.get('sectionName')
        rep_lines = []
        if sec_title:
            rep_lines.append(Utils.gen_report_sec_header(sec_title, title_width=20))
        if sec_conf.get('content'):
            headers = sec_conf['content'].get('header')
            if headers:
                rep_lines.extend(headers)
            all_lines = self._generate_section_lines(sec_conf)
            if all_lines:
                rep_lines.extend(all_lines)
        return rep_lines

    def _generate_platform_report_sections(self) -> List[str]:
        section_arr = self.ctxt.platform.configs.get_value_silent('wrapperReporting',
                                                                  self.name,
                                                                  'sections')
        if section_arr:
            rep_lines = []
            for curr_sec in section_arr:
                rep_lines.extend(self._generate_section_content(curr_sec))
            return rep_lines
        return None


@dataclass
class RapidsJarTool(RapidsTool):
    """
    A wrapper class to represent wrapper commands that require RAPIDS jar file.
    """

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
        defined_tool_options = self.ctxt.get_value_silent('sparkRapids', 'cli', 'toolOptions')
        if defined_tool_options is not None:
            if option_key not in defined_tool_options:
                self.logger.warning('Ignoring tool option [%s]. Invalid option.', option_key)
                return False
        return True

    def _process_tool_args_from_input(self) -> list:
        """
        Process the arguments passed from the CLI if any and return a list of strings representing
        the arguments to be passed to the final command running the job. This needs processing
        because we need to verify the arguments and handle hiphens
        :return: list of the rapids arguments added by the user
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
        return arguments_list

    def _process_tool_args(self):
        """
        Process the arguments passed from the CLI if any and return a string representing the
        arguments to be passed to the final command running the job.
        :return:
        """
        self.ctxt.add_rapids_args('rapidsOpts', self._process_tool_args_from_input())

    def _process_dependencies(self):
        """
        For local deployment mode, we need to process the extra dependencies specific to the platform
        :return:
        """
        if 'deployMode' in self.ctxt.platform_opts:
            # process the deployment
            # we need to download the dependencies locally if necessary
            self._download_dependencies()

    def timeit(timed_item: str):  # pylint: disable=no-self-argument
        def decorator(func_cb: Callable):
            def wrapper(self, *args, **kwargs):
                start_time = time.monotonic()
                func_cb(self, *args, **kwargs)  # pylint: disable=not-callable
                end_time = time.monotonic()
                self.logger.info('Total Execution Time: %s => %s seconds', timed_item,
                                 f'{(end_time-start_time):,.3f}')
            return wrapper
        return decorator

    @timeit('Downloading dependencies for local Mode')  # pylint: disable=too-many-function-args
    def _download_dependencies(self):

        def exception_handler(future):
            # Handle any exceptions raised by the task
            exception = future.exception()
            if exception:
                self.logger.error('Error while downloading dependency: %s', exception)

        def cache_single_dependency(dep: dict) -> str:
            """
            Downloads the specified URL and saves it to disk
            """
            start_time = time.monotonic()
            self.logger.info('Checking dependency %s', dep['name'])
            dest_folder = self.ctxt.get_cache_folder()
            resource_file_name = FSUtil.get_resource_name(dep['uri'])
            resource_file = FSUtil.build_path(dest_folder, resource_file_name)
            file_check_dict = {'size': dep['size']}
            is_created = FSUtil.cache_from_url(dep['uri'], resource_file, file_checks=file_check_dict)
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
            end_time = time.monotonic()
            self.logger.info('Completed downloading of dependency [%s] => %s seconds',
                             dep['name'],
                             f'{(end_time-start_time):,.3f}')
            return dep_item

        def cache_all_dependencies(dep_arr: List[dict]):
            """
            Create a thread pool and download specified urls
            """
            futures_list = []
            results = []
            with ThreadPoolExecutor(max_workers=4) as executor:
                for dep in dep_arr:
                    futures = executor.submit(cache_single_dependency, dep)
                    futures.add_done_callback(exception_handler)
                    futures_list.append(futures)
                try:
                    # set the timeout to 30 minutes.
                    for future in concurrent.futures.as_completed(futures_list, timeout=1800):
                        result = future.result()
                        results.append(result)
                except Exception as ex:    # pylint: disable=broad-except
                    self.logger.error('Failed to download dependencies %s', ex)
                    raise ex
            return results

        # TODO: Verify the downloaded file by checking their MD5
        deploy_mode = DeployMode.tostring(self.ctxt.get_deploy_mode())
        depend_arr = self.ctxt.platform.configs.get_value_silent('dependencies',
                                                                 'deployMode',
                                                                 deploy_mode)
        if depend_arr:
            dep_list = cache_all_dependencies(depend_arr)
            if any(dep_item is None for dep_item in dep_list):
                raise RuntimeError('Could not download all dependencies. Aborting Executions.')
            self.logger.info('Dependencies are processed as: %s',
                             Utils.gen_joined_str(join_elem='; ',
                                                  items=dep_list))
            self.ctxt.add_rapids_args('javaDependencies', dep_list)

    def _process_rapids_args(self):
        # add a dictionary to hold the rapids arguments
        self._process_jar_arg()
        self._process_dependencies()
        self._process_tool_args()

    def _process_offline_cluster_args(self):
        pass

    def _process_gpu_cluster_args(self, offline_cluster_opts: dict = None) -> bool:
        pass

    def _copy_dependencies_to_remote(self):
        self.logger.info('Skipping preparing remote dependency folder')

    def _prepare_job_arguments(self):
        self._prepare_local_job_arguments()

    def _run_rapids_tool(self):
        # 1- copy dependencies to remote server
        self._copy_dependencies_to_remote()
        # 2- prepare the arguments
        #  2.a -check if the app_id is not none
        self._prepare_job_arguments()
        #
        # 3- create a submission job
        # 4- execute

    def _get_main_cluster_obj(self):
        return self.ctxt.get_ctxt('cpuClusterProxy')

    def _process_eventlogs_args(self):
        eventlog_arg = self.wrapper_options.get('eventlogs')
        if eventlog_arg is None:
            # get the eventlogs from spark properties
            cpu_cluster_obj = self._get_main_cluster_obj()
            if cpu_cluster_obj:
                spark_event_logs = cpu_cluster_obj.get_eventlogs_from_config()
            else:
                self.logger.warning('Eventlogs is not set properly. The property cannot be pulled '
                                    'from cluster because it is not defined')
                spark_event_logs = []
        else:
            if isinstance(eventlog_arg, tuple):
                spark_event_logs = List[eventlog_arg]
            elif isinstance(eventlog_arg, str):
                spark_event_logs = eventlog_arg.split(',')
            else:
                spark_event_logs = eventlog_arg
        if len(spark_event_logs) < 1:
            self.logger.error('Eventlogs list is empty. '
                              'The cluster Spark properties may be missing "spark.eventLog.dir". '
                              'Re-run the command passing "--eventlogs" flag to the wrapper.')
            raise RuntimeError('Invalid arguments. The list of Apache Spark event logs is empty.')
        self.ctxt.set_ctxt('eventLogs', spark_event_logs)

    def _create_migration_cluster(self, cluster_type: str, cluster_arg: str) -> ClusterBase:
        if cluster_arg is None:
            raise RuntimeError(f'The {cluster_type} cluster argument is not set.')
        arg_is_file = self.ctxt.platform.storage.is_file_path(cluster_arg)
        if not arg_is_file:
            self.logger.info('Loading %s cluster properties by name %s. Note that this will fail '
                             'if the cluster was permanently deleted.',
                             cluster_type,
                             cluster_arg)
            # create a cluster by name
            cluster_obj = self.ctxt.platform.connect_cluster_by_name(cluster_arg)
        else:
            self.logger.info('Loading %s cluster properties from file %s',
                             cluster_type,
                             cluster_arg)
            # create cluster by loading properties files
            # download the file to the working directory
            cluster_conf_path = self.ctxt.platform.storage.download_resource(cluster_arg,
                                                                             self.ctxt.get_local_work_dir())
            cluster_obj = self.ctxt.platform.load_cluster_by_prop_file(cluster_conf_path)
        return cluster_obj

    def _gen_output_tree(self) -> List[str]:
        tree_conf = self.ctxt.get_value('local', 'output', 'treeDirectory')
        if tree_conf and tree_conf.get('enabled'):
            level = tree_conf.get('depthLevel')
            indentation = tree_conf.get('indentation', '\t')
            ex_patterns = tree_conf.get('excludedPatterns', {})
            exc_dirs = ex_patterns.get('directories')
            exc_files = ex_patterns.get('files')
            out_folder_path = self.ctxt.get_local('outputFolder')
            out_tree_list = FSUtil.gen_dir_tree(out_folder_path,
                                                depth_limit=level,
                                                indent=indentation,
                                                exec_dirs=exc_dirs,
                                                exec_files=exc_files)
            doc_url = self.ctxt.get_value('sparkRapids', 'outputDocURL')
            out_tree_list.append(f'{indentation}- To learn more about the output details, visit {doc_url}')
            return out_tree_list
        return None

    def _report_tool_full_location(self) -> str:
        if not self._rapids_jar_tool_has_output():
            return None
        out_folder_path = self.ctxt.get_rapids_output_folder()
        res_arr = [Utils.gen_report_sec_header('Output'),
                   f'{self.pretty_name()} tool output: {out_folder_path}']
        out_tree_list = self._gen_output_tree()
        return Utils.gen_multiline_str(res_arr, out_tree_list)

    def _evaluate_rapids_jar_tool_output_exist(self) -> bool:
        """
        Used as a subtask of self._process_output(). this method has the responsibility of
        checking if the tools produced no output and take the necessary action
        :return: True if the tool has generated an output
        """
        rapids_output_dir = self.ctxt.get_rapids_output_folder()
        res = True
        if not self.ctxt.platform.storage.resource_exists(rapids_output_dir):
            res = False
            self.ctxt.set_ctxt('wrapperOutputContent',
                               self._report_results_are_empty())
            self.logger.info('The Rapids jar tool did not generate an output directory')
        self.ctxt.set_ctxt('rapidsOutputIsGenerated', res)
        return res

    def _rapids_jar_tool_has_output(self) -> bool:
        return self.ctxt.get_ctxt('rapidsOutputIsGenerated')

    @timeit('Processing job submission arguments')  # pylint: disable=too-many-function-args
    def _process_job_submission_args(self):
        self._process_local_job_submission_args()

    def _set_remote_folder_for_submission(self, requires_remote_storage: bool) -> dict:
        res = {}
        submission_args = self.wrapper_options.get('jobSubmissionProps')
        # get the root remote folder and make sure it exists
        remote_folder = submission_args.get('remoteFolder')
        # If remote_folder is not specified, then ignore it
        if requires_remote_storage:
            # if the remote storage required and no remote folder specified. then try to assign the
            # tmp storage of the exec_cluster to be used for storage
            archive_enabled = True
            if not remote_folder:
                # get the execCluster
                exec_cluster = self.get_exec_cluster()
                if exec_cluster:
                    remote_folder = exec_cluster.get_tmp_storage()
                    if remote_folder:
                        archive_enabled = False
            self.ctxt.set_ctxt('archiveToRemote', archive_enabled)
        if remote_folder is None:
            # the output is only for local machine
            self.logger.info('No remote output folder specified.')
            if requires_remote_storage:
                raise RuntimeError(f'Remote folder [{remote_folder}] is invalid.')
        else:
            if not self.ctxt.platform.storage.resource_exists(remote_folder):
                raise RuntimeError(f'Remote folder invalid path. [{remote_folder}] does not exist.')
            # now we should make the subdirectory to indicate the output folder,
            # by appending the name of the execution folder
            exec_full_name = self.ctxt.get_ctxt('execFullName')
            remote_workdir = FSUtil.build_url_from_parts(remote_folder, exec_full_name)
            self.ctxt.set_remote('rootFolder', remote_folder)
            self.ctxt.set_remote('workDir', remote_workdir)
            self.logger.info('Remote workdir is set as %s', remote_workdir)
            remote_dep_folder = FSUtil.build_url_from_parts(remote_workdir,
                                                            self.ctxt.get_ctxt('depFolderName'))
            self.ctxt.set_remote('depFolder', remote_dep_folder)
            self.logger.info('Remote dependency folder is set as %s', remote_dep_folder)
        if requires_remote_storage:
            res.update({'outputDirectory': self.ctxt.get_remote('workDir')})
        else:
            # the output folder has to be set any way
            res.update({'outputDirectory': self.ctxt.get_output_folder()})
        return res

    def _process_local_job_submission_args(self):
        job_args = {}
        submission_args = self.wrapper_options.get('jobSubmissionProps')
        job_args.update(self._set_remote_folder_for_submission(self.requires_remote_folder()))
        platform_args = submission_args.get('platformArgs')
        if platform_args is not None:
            processed_platform_args = self.ctxt.platform.cli.build_local_job_arguments(platform_args)
            ctxt_rapids_args = self.ctxt.get_ctxt('rapidsArgs')
            dependencies = ctxt_rapids_args.get('javaDependencies')
            processed_platform_args.update({'dependencies': dependencies})
            job_args['platformArgs'] = processed_platform_args
        self.ctxt.update_job_args(job_args)

    def _init_rapids_arg_list(self) -> List[str]:
        return []

    @timeit('Building Job Arguments and Executing Job CMD')  # pylint: disable=too-many-function-args
    def _prepare_local_job_arguments(self):
        job_args = self.ctxt.get_ctxt('jobArgs')
        # now we can create the job object
        # Todo: For dataproc, this can be autogenerated from cluster name
        rapids_arg_list = self._init_rapids_arg_list()
        ctxt_rapids_args = self.ctxt.get_ctxt('rapidsArgs')
        jar_file_path = ctxt_rapids_args.get('jarFilePath')
        rapids_opts = ctxt_rapids_args.get('rapidsOpts')
        if rapids_opts:
            rapids_arg_list.extend(rapids_opts)
        # add the eventlogs at the end of all the tool options
        rapids_arg_list.extend(self.ctxt.get_ctxt('eventLogs'))
        class_name = self.ctxt.get_value('sparkRapids', 'mainClass')
        rapids_arg_obj = {
            'jarFile': jar_file_path,
            'jarArgs': rapids_arg_list,
            'className': class_name
        }
        platform_args = job_args.get('platformArgs')
        spark_conf_args = {}
        job_properties_json = {
            'outputDirectory': job_args.get('outputDirectory'),
            'rapidsArgs': rapids_arg_obj,
            'sparkConfArgs': spark_conf_args,
            'platformArgs': platform_args
        }
        job_properties = RapidsJobPropContainer(prop_arg=job_properties_json,
                                                file_load=False)
        job_obj = self.ctxt.platform.create_local_submission_job(job_prop=job_properties,
                                                                 ctxt=self.ctxt)
        job_obj.run_job()

    def _archive_results(self):
        self._archive_local_results()

    def _archive_local_results(self):
        remote_work_dir = self.ctxt.get_remote('workDir')
        if remote_work_dir and self._rapids_jar_tool_has_output():
            local_folder = self.ctxt.get_output_folder()
            # TODO make sure it worth issuing the command
            self.ctxt.platform.storage.upload_resource(local_folder, remote_work_dir)
