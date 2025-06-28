# Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

"""Base class representing wrapper around the QualX tool."""

import os
from dataclasses import dataclass

from spark_rapids_pytools.cloud_api.sp_types import get_platform
from spark_rapids_pytools.common.sys_storage import FSUtil
from spark_rapids_pytools.common.utilities import Utils
from spark_rapids_pytools.rapids.rapids_tool import RapidsTool
from spark_rapids_pytools.rapids.tool_ctxt import ToolContext


@dataclass
class QualXTool(RapidsTool):
    """
    Base class for QualX tool. This class overrides methods from RapidsTool that are not
    applicable to the QualX tool.
    """
    def _check_environment(self):
        pass

    def _connect_to_execution_cluster(self):
        pass

    def _init_ctxt(self):
        """
        Initialize the tool context, reusing qualification configurations.
        TODO: We should use qualx_conf.yaml instead of qualification-conf.yaml
        """
        self.config_path = Utils.resource_path('qualification-conf.yaml')
        self.ctxt = ToolContext(platform_cls=get_platform(self.platform_type),
                                platform_opts=self.wrapper_options.get('platformOpts'),
                                prop_arg=self.config_path,
                                name=self.name)

    def process_qualx_env_vars(self):
        """
        Process environment variables required for QualX tool execution.

        This method ensures that SPARK_RAPIDS_TOOLS_JAR environment variable is set.
        Precedence order:
        1. Existing SPARK_RAPIDS_TOOLS_JAR environment variable (highest priority)
        2. Tools jar from local wheel package resources (fallback)

        The environment variable is required by the run_profiler_tool function used during training.
        """
        # Priority 1: Check if environment variable is already set - this takes precedence
        if 'SPARK_RAPIDS_TOOLS_JAR' in os.environ:
            existing_jar = os.environ['SPARK_RAPIDS_TOOLS_JAR']
            if os.path.exists(existing_jar):
                self.logger.info('Using existing SPARK_RAPIDS_TOOLS_JAR environment variable (priority): %s',
                                 existing_jar)
                return
            self.logger.warning('SPARK_RAPIDS_TOOLS_JAR points to non-existent file: %s.'
                                'Will fallback to resource jar.', existing_jar)

        # Priority 2: Fallback to identifying tools jar from local resources
        self.logger.info('No valid SPARK_RAPIDS_TOOLS_JAR found. Loading from tools resources...')
        self.ctxt.load_tools_jar_resources()

        if self.ctxt.use_local_tools_jar():
            jar_path = self.ctxt.get_rapids_jar_url()
            os.environ['SPARK_RAPIDS_TOOLS_JAR'] = jar_path
            self.logger.info('Set SPARK_RAPIDS_TOOLS_JAR environment variable to: %s', jar_path)
        else:
            self.logger.error('Failed to identify tools jar from resources. '
                              'Please set SPARK_RAPIDS_TOOLS_JAR environment variable manually.')
            raise RuntimeError('Unable to locate tools jar for QualX execution')

    def _process_rapids_args(self):
        """Process RAPIDS arguments and environment variables for QualX tool."""
        self.process_qualx_env_vars()

    def _process_output_args(self):
        """
        Sets the `output_folder`, ensures its creation, and updates the context with the folder path.
        """
        self.logger.debug('Processing Output Arguments')
        if self.output_folder is None:
            self.output_folder = os.getcwd()
        self.output_folder = FSUtil.get_abs_path(self.output_folder)
        exec_dir_name = f'{self.name}_{self.ctxt.uuid}'
        # It should never happen that the exec_dir_name exists
        self.output_folder = FSUtil.build_path(self.output_folder, exec_dir_name)
        FSUtil.make_dirs(self.output_folder, exist_ok=False)
        self.ctxt.set_local('outputFolder', self.output_folder)
        self.logger.info('Local output folder is set as: %s', self.output_folder)

    def _collect_result(self):
        pass

    def _archive_phase(self):
        pass

    def _finalize(self):
        pass
