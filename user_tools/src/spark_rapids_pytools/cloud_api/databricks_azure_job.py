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
        key = ""
        if 'key' in self.exec_ctxt.platform.ctxt:
            key = self.exec_ctxt.platform.ctxt['key']
        else:
            eventlogs = self.exec_ctxt.get_value('wrapperCtx', 'eventLogs')
            if eventlogs and len(eventlogs) > 0:
                key = get_account_name(eventlogs[0])
        if key != "":
            vm_args.append(f'-Dspark.hadoop.fs.azure.account.key.databricksazuretest.dfs.core.windows.net={key}')
        return vm_args
