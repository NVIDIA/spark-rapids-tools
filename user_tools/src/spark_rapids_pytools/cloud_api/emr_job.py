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

"""Implementation of Job submissions on EMR"""

from dataclasses import dataclass
from spark_rapids_pytools.rapids.rapids_job import RapidsLocalJob


@dataclass
class EmrLocalRapidsJob(RapidsLocalJob):
    """
    Implementation of a RAPIDS job that runs local on a local machine.
    """
    job_label = 'emrLocal'

    def _build_submission_cmd(self) -> list:
        # env vars are added later as a separate dictionary
        cmd_arg = super()._build_submission_cmd()
        # any s3 link has to be converted to S3a:
        for index, arr_entry in enumerate(cmd_arg):
            cmd_arg[index] = arr_entry.replace('s3://', 's3a://')
        return cmd_arg
