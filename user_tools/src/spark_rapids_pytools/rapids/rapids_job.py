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

"""Abstract representation of a wrapper Job"""

from dataclasses import dataclass, field
from logging import Logger

from spark_rapids_pytools.common.prop_manager import JSONPropertiesContainer
from spark_rapids_pytools.common.utilities import ToolLogging
from spark_rapids_pytools.rapids.tool_ctxt import ToolContext


@dataclass
class RapidsJobPropContainer(JSONPropertiesContainer):
    """
    Container to manage the properties and arguments needed to submit a job running RAPIDS plugin.
    """

    def _init_fields(self):
        if self.get_value_silent('rapidsArgs') is None:
            self.props['rapidsArgs'] = {}
        if self.get_value_silent('sparkConfArgs') is None:
            self.props['sparkConfArgs'] = {}
        if self.get_value_silent('platformArgs') is None:
            self.props['platformArgs'] = {}

    def get_jar_file(self):
        return self.get_value('rapidsArgs', 'jarFile')

    def get_jar_main_class(self):
        return self.get_value('rapidsArgs', 'className')

    def get_rapids_args(self):
        return self.get_value('rapidsArgs', 'jarArgs')


@dataclass
class RapidsJob:
    """
    A wrapper class to represent the actual execution of a RAPIDS plugin job on the cloud platform.
    """
    prop_container: RapidsJobPropContainer
    exec_ctxt: ToolContext
    remote_output: str = field(default=None, init=False)
    job_label: str = field(default=None, init=False)
    logger: Logger = field(default=None, init=False)

    def _init_fields(self):
        self.logger = ToolLogging.get_and_setup_logger(f'rapids.tools.submit.{self.job_label}')
        self.remote_output = self.prop_container.get_value_silent('remoteOutput')

    def __post_init__(self):
        self._init_fields()

    def _get_rapids_args_per_platform(self):
        return []

    def _build_rapids_args(self):
        rapids_args = self._get_rapids_args_per_platform()[:]
        rapids_args.extend(['--output-directory', self.remote_output])
        return rapids_args

    def _build_submission_cmd(self):
        raise NotImplementedError

    def _submit_job(self, cmd_args: list) -> str:
        raise NotImplementedError

    def _print_job_output(self, job_output: str):
        stdout_splits = job_output.splitlines()
        if len(stdout_splits) > 0:
            std_out_lines = '\n'.join([f'\t| {line}' for line in stdout_splits])
            stdout_str = f'\n\t<STDOUT>\n{std_out_lines}'
            self.logger.info('EMR Job output:%s', stdout_str)

    def run_job(self):
        self.logger.info('Prepare job submission command')
        cmd_args = self._build_submission_cmd()
        job_output = self._submit_job(cmd_args)
        if not ToolLogging.is_debug_mode_enabled():
            # we check the debug level because we do not want the output to be displayed twice
            self._print_job_output(job_output)
        return job_output
