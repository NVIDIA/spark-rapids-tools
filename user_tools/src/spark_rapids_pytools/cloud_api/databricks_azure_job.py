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

"""Implementation of Job submissions on Databricks Azure"""

from dataclasses import dataclass

from spark_rapids_pytools.common.prop_manager import JSONPropertiesContainer
from spark_rapids_pytools.rapids.rapids_job import RapidsLocalJob


@dataclass
class DBAzureLocalRapidsJob(RapidsLocalJob):
    """
    Implementation of a RAPIDS job that runs local on a local machine.
    """
    job_label = 'DBAzureLocal'

    @classmethod
    def get_account_name(cls, url: str):
        return url.split('@')[1].split('.')[0]

    def _build_jvm_args(self):
        vm_args = super()._build_jvm_args()
        key = ''
        account_name = ''

        eventlogs = self.exec_ctxt.get_value('wrapperCtx', 'eventLogs')
        if eventlogs:
            account_name = self.get_account_name(eventlogs[0])
            cmd_args = ['az storage account show-connection-string', '--name', account_name]
            std_out = self.exec_ctxt.platform.cli.run_sys_cmd(cmd_args)
            conn_str = JSONPropertiesContainer(prop_arg=std_out, file_load=False).get_value('connectionString')
            key = conn_str.split('AccountKey=')[1].split(';')[0]

        if key and account_name:
            vm_args.append(f'-Drapids.tools.hadoop.fs.azure.account.key.{account_name}.dfs.core.windows.net={key}')

        return vm_args
