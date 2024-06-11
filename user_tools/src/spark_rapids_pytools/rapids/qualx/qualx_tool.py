# Copyright (c) 2024, NVIDIA CORPORATION.
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
