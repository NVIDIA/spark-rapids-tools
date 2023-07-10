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
from typing import List

from spark_rapids_pytools.common.prop_manager import JSONPropertiesContainer
from spark_rapids_pytools.common.utilities import ToolLogging, Utils
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
    output_path: str = field(default=None, init=False)
    job_label: str = field(default=None, init=False)
    logger: Logger = field(default=None, init=False)

    def get_platform_name(self):
        return self.exec_ctxt.get_platform_name()

    def _init_fields(self):
        self.logger = ToolLogging.get_and_setup_logger(f'rapids.tools.submit.{self.job_label}')
        self.output_path = self.prop_container.get_value_silent('outputDirectory')

    def __post_init__(self):
        self._init_fields()

    def _get_rapids_args_per_platform(self) -> List[str]:
        """Left as placeholder for future use"""
        return []

    def _get_persistent_rapids_args(self):
        rapids_args = self._get_rapids_args_per_platform()[:]
        rapids_args.extend(['--output-directory', self.output_path])
        return rapids_args

    def _build_rapids_args(self):
        rapids_arguments = self._get_persistent_rapids_args()
        extra_rapids_args = self.prop_container.get_rapids_args()
        if extra_rapids_args is None:
            return rapids_arguments
        rapids_arguments.extend(extra_rapids_args)
        return rapids_arguments

    def _build_submission_cmd(self) -> list:
        raise NotImplementedError

    def _submit_job(self, cmd_args: list) -> str:
        raise NotImplementedError

    def _print_job_output(self, job_output: str):
        stdout_splits = job_output.splitlines()
        if len(stdout_splits) > 0:
            std_out_lines = Utils.gen_multiline_str([f'\t| {line}' for line in stdout_splits])
            stdout_str = f'\n\t<STDOUT>\n{std_out_lines}'
            self.logger.info('%s job output:%s', self.get_platform_name(), stdout_str)

    def run_job(self):
        self.logger.info('Prepare job submission command')
        cmd_args = self._build_submission_cmd()
        self.logger.info('Running the Rapids Job...')
        job_output = self._submit_job(cmd_args)
        if not ToolLogging.is_debug_mode_enabled():
            # we check the debug level because we do not want the output to be displayed twice
            self._print_job_output(job_output)
        return job_output


@dataclass
class RapidsLocalJob(RapidsJob):
    """
    Implementation of a RAPIDS job that runs local on a local machine.
    """

    def _build_classpath(self):
        deps_arr = [self.prop_container.get_jar_file()]
        dependencies = self.prop_container.get_value_silent('platformArgs', 'dependencies')
        if dependencies is not None:
            deps_arr.extend(dependencies)
        dps_str = Utils.gen_joined_str(':', deps_arr)
        return ['-cp', dps_str]

    def _build_jvm_args(self):
        jvm_args = self.prop_container.get_value_silent('platformArgs', 'jvmArgs')
        vm_args = []
        if jvm_args is not None:
            for jvm_k, jvm_arg in jvm_args.items():
                if jvm_k.startswith('D'):
                    val = f'-{jvm_k}={jvm_arg}'
                else:
                    val = f'-{jvm_k}'
                vm_args.append(val)
        return vm_args

    def _build_submission_cmd(self) -> list:
        # env vars are added later as a separate dictionary
        classpath_arr = self._build_classpath()
        jvm_args_arr = self._build_jvm_args()
        cmd_arg = ['java']
        cmd_arg.extend(jvm_args_arr)
        cmd_arg.extend(classpath_arr)
        cmd_arg.append(self.prop_container.get_jar_main_class())
        cmd_arg.extend(self._build_rapids_args())
        return cmd_arg

    def _submit_job(self, cmd_args: list) -> str:
        env_args = self.prop_container.get_value_silent('platformArgs', 'envArgs')
        out_std = self.exec_ctxt.platform.cli.run_sys_cmd(cmd=cmd_args,
                                                          env_vars=env_args)
        return out_std
