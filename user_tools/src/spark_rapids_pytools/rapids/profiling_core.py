# Copyright (c) 2025, NVIDIA CORPORATION.
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

"""Core implementation class for execution of profiling tool."""

from dataclasses import dataclass
from typing import List

from spark_rapids_pytools.rapids.rapids_tool import RapidsJarTool


@dataclass
class ProfilingCore(RapidsJarTool):
    """
    Core profiling tool
    """
    name = 'profiling'

    def _process_custom_args(self) -> None:
        self._process_eventlogs_args()

    def _init_rapids_arg_list(self) -> List[str]:
        return super()._init_rapids_arg_list() + ['--csv', '--output-sql-ids-aligned']

    def _process_output(self) -> None:
        if not self._evaluate_rapids_jar_tool_output_exist():
            self.logger.warning('No output files found from profiling core tool')
        else:
            self.logger.info('Profiling core tool completed successfully')

    def _download_remote_output_folder(self):
        self.logger.debug('Local mode skipping downloading the remote output workdir')

    def _delete_remote_dep_folder(self):
        self.logger.debug('Local mode skipping deleting the remote workdir')

    def _process_job_submission_args(self):
        self._process_local_job_submission_args()

    def _prepare_job_arguments(self):
        super()._prepare_local_job_arguments()
