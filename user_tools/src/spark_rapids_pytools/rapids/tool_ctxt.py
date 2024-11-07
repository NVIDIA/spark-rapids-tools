# Copyright (c) 2023-2024, NVIDIA CORPORATION.
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

"""Implementation of class holding the execution context of a rapids tool"""

import os
import re
import tarfile
from dataclasses import dataclass, field
from logging import Logger
from typing import Type, Any, ClassVar, List

from spark_rapids_pytools.cloud_api.sp_types import PlatformBase
from spark_rapids_pytools.common.prop_manager import YAMLPropertiesContainer
from spark_rapids_pytools.common.sys_storage import FSUtil
from spark_rapids_pytools.common.utilities import ToolLogging, Utils
from spark_rapids_tools import CspEnv, CspPath
from spark_rapids_tools.utils import Utilities


@dataclass
class ToolContext(YAMLPropertiesContainer):
    """
    A container that holds properties and characteristics of a given execution.
    """
    name: str = None
    platform_cls: Type[PlatformBase] = None
    platform_opts: dict = field(default_factory=dict)
    logger: Logger = field(default=None, init=False)
    platform: PlatformBase = field(default=None, init=False)
    uuid: str = field(default=None, init=False)
    prepackage_paths: ClassVar[List[str]] = [
        Utils.resource_path('csp-resources.tgz'),
        Utils.resource_path('csp-resources')
    ]

    @classmethod
    def are_resources_prepackaged(cls) -> bool:
        return any(os.path.exists(f) for f in cls.prepackage_paths)

    def __connect_to_platform(self):
        self.logger.info('Start connecting to the platform')
        self.platform = self.platform_cls(ctxt_args=self.platform_opts)

    def __create_and_set_uuid(self):
        # For backward compatibility, we still generate the uuid with timestamp if
        # the environment variable is not set.
        self.uuid = Utils.get_rapids_tools_env('UUID', Utils.gen_uuid_with_ts(suffix_len=8))

    def __create_and_set_cache_folder(self):
        # get the cache folder from environment variables or set it to default
        cache_folder = Utils.get_rapids_tools_env('CACHE_FOLDER', '/var/tmp/spark_rapids_user_tools_cache')
        # make sure the environment is set
        Utils.set_rapids_tools_env('CACHE_FOLDER', cache_folder)
        FSUtil.make_dirs(cache_folder)
        self.set_local('cacheFolder', cache_folder)

    def get_cache_folder(self) -> str:
        return self.get_local('cacheFolder')

    def _init_fields(self):
        self.logger = ToolLogging.get_and_setup_logger(f'rapids.tools.{self.name}.ctxt')
        self.__connect_to_platform()
        self.__create_and_set_uuid()
        self.props['localCtx'] = {}
        self.props['remoteCtx'] = {}
        self.props['wrapperCtx'] = {}
        # add a dictionary that holds all the rapids plugin args
        self.props['wrapperCtx']['rapidsArgs'] = {}
        # add a dictionary that holds arguments to be passed to the plugin args
        self.props['wrapperCtx']['jobArgs'] = {}
        # create cache_folder that will be used to hold large downloaded files
        self.__create_and_set_cache_folder()

    def get_deploy_mode(self) -> Any:
        return self.platform_opts.get('deployMode')

    def is_fat_wheel_mode(self) -> bool:
        return self.get_ctxt('fatWheelModeEnabled')

    def set_ctxt(self, key: str, val: Any):
        self.props['wrapperCtx'][key] = val

    def add_rapids_args(self, key: str, val: Any):
        self.props['wrapperCtx']['rapidsArgs'][key] = val

    def add_job_args(self, key: str, val: Any):
        self.props['wrapperCtx']['jobArgs'][key] = val

    def update_job_args(self, extra_args: dict):
        self.props['wrapperCtx']['jobArgs'].update(extra_args)

    def get_ctxt(self, key: str):
        return self.props['wrapperCtx'].get(key)

    def set_remote(self, key: str, val: Any):
        self.props['remoteCtx'][key] = val

    def set_local(self, key: str, val: Any):
        self.props['localCtx'][key] = val

    def get_local(self, key: str):
        return self.props['localCtx'].get(key)

    def get_remote(self, key: str):
        return self.props['remoteCtx'].get(key)

    def set_local_workdir(self, parent: str):
        short_name = self.get_value('platform', 'shortName')
        exec_dir_name = f'{short_name}_{self.uuid}'
        self.set_ctxt('execFullName', exec_dir_name)
        exec_root_dir = FSUtil.build_path(parent, exec_dir_name)
        self.logger.info('Local workdir root folder is set as %s', exec_root_dir)
        # It should never happen that the exec_root_dir exists
        FSUtil.make_dirs(exec_root_dir, exist_ok=False)
        # Create the dependency folder. It is a subdirectory in the output folder
        # because we want that same name appear on the remote storage when copying
        dep_folder_name = 'work_dir'
        self.set_ctxt('depFolderName', dep_folder_name)
        dep_folder = FSUtil.build_path(exec_root_dir, dep_folder_name)
        FSUtil.make_dirs(dep_folder, exist_ok=False)
        self.set_local('outputFolder', exec_root_dir)
        self.set_local('depFolder', dep_folder)
        self.logger.info('Dependencies are generated locally in local disk as: %s', dep_folder)
        self.logger.info('Local output folder is set as: %s', exec_root_dir)

    def _identify_fat_wheel_jar(self, resource_files: List[str]) -> None:
        """
        Identifies the tools JAR file from resource files in fat wheel mode and sets its name in the context.
        :param resource_files: List of resource files to search for the tools JAR file.
        :raises AssertionError: If the number of matching files is not exactly one.
        """
        tools_jar_regex_str = self.get_value('sparkRapids', 'toolsJarRegex')
        tools_jar_regex = re.compile(tools_jar_regex_str)
        matched_files = [f for f in resource_files if tools_jar_regex.search(f)]
        assert len(matched_files) == 1, \
            (f'Expected exactly one tools JAR file, found {len(matched_files)}. '
             'Rebuild the wheel package with the correct tools JAR file.')
        # set the tools JAR file name in the context
        self.set_ctxt('fatWheelModeJarFileName', FSUtil.get_resource_name(matched_files[0]))

    def load_prepackaged_resources(self):
        """
        Checks if the packaging includes the CSP dependencies. If so, it moves the dependencies
        into the tmp folder. This allows the tool to pick the resources from cache folder.
        """
        if not self.are_resources_prepackaged():
            return
        self.set_ctxt('fatWheelModeEnabled', True)
        self.logger.info(Utils.gen_str_header('Fat Wheel Mode Is Enabled',
                                              ruler='_', line_width=50))

        for res_path in self.prepackage_paths:
            if os.path.exists(res_path):
                if os.path.isdir(res_path):
                    # this is a directory, copy all the contents to the tmp
                    FSUtil.copy_resource(res_path, self.get_cache_folder())
                    self._identify_fat_wheel_jar(FSUtil.get_all_files(res_path))
                else:
                    # this is an archived file
                    with tarfile.open(res_path, mode='r:*') as tar_file:
                        tar_file.extractall(self.get_cache_folder())
                        self._identify_fat_wheel_jar(tar_file.getnames())
                        tar_file.close()

    def get_output_folder(self) -> str:
        return self.get_local('outputFolder')

    def get_wrapper_summary_file_path(self) -> str:
        summary_file_name = self.get_value('local', 'output', 'fileName')
        summary_path = FSUtil.build_path(self.get_output_folder(), summary_file_name)
        return summary_path

    def get_local_work_dir(self) -> str:
        return self.get_local('depFolder')

    def get_rapids_jar_url(self) -> str:
        self.logger.info('Fetching the Rapids Jar URL')
        # get the version from the package, instead of the yaml file
        # jar_version = self.get_value('sparkRapids', 'version')
        if self.is_fat_wheel_mode():
            return self._get_tools_jar_in_fat_wheel_mode()
        mvn_base_url = self.get_value('sparkRapids', 'mvnUrl')
        jar_version = Utilities.get_latest_mvn_jar_from_metadata(mvn_base_url)
        rapids_url = self.get_value('sparkRapids', 'repoUrl').format(mvn_base_url, jar_version, jar_version)
        return rapids_url

    def get_tool_main_class(self) -> str:
        return self.get_value('sparkRapids', 'mainClass')

    def get_rapids_auto_tuner_enabled(self) -> bool:
        return self.get_value('sparkRapids', 'enableAutoTuner')

    def requires_eventlogs(self) -> bool:
        flag = self.get_value_silent('sparkRapids', 'requireEventLogs')
        if flag is None:
            return True
        return flag

    def get_rapids_output_folder(self) -> str:
        root_dir = self.get_local('outputFolder')
        rapids_subfolder = self.get_value_silent('toolOutput', 'subFolder')
        if rapids_subfolder is None:
            return root_dir
        return FSUtil.build_path(root_dir, rapids_subfolder)

    def get_metrics_output_folder(self) -> CspPath:
        root_dir = CspPath(self.get_rapids_output_folder())
        metrics_subfolder = self.get_value('toolOutput', 'metricsSubFolder')
        return root_dir.create_sub_path(metrics_subfolder)

    def get_log4j_properties_file(self) -> str:
        return self.get_value_silent('toolOutput', 'textFormat', 'log4jFileName')

    def get_platform_name(self) -> str:
        """
        This used to get the lower case of the platform of the runtime.
        :return: the name of the platform of the runtime in lower_case.
        """
        return CspEnv.pretty_print(self.platform.type_id)

    def _get_tools_jar_in_fat_wheel_mode(self) -> str:
        """
        Extracts the tools JAR file from the context and returns its path from the cache folder.
        """
        jar_filename = self.get_ctxt('fatWheelModeJarFileName')
        if jar_filename is None:
            raise ValueError(
                'In Fat Mode. Tools JAR file name not found in context. '
                'Rebuild the wheel package or re-run without fat wheel mode.'
            )
        # construct the path to the tools JAR file in the cache folder
        jar_filepath = FSUtil.build_path(self.get_cache_folder(), jar_filename)
        if not FSUtil.resource_exists(jar_filepath):
            raise FileNotFoundError(
                f'In Fat Mode. Tools JAR not found in cache folder: {jar_filepath}. '
                'Rebuild the wheel package or re-run without fat wheel mode.'
            )
        self.logger.info('Using jar from wheel file %s', jar_filepath)
        return jar_filepath
