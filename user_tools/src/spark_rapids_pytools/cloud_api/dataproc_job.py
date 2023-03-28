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

"""Implementation of Job submissions on GCloud Dataproc"""

from dataclasses import dataclass

from spark_rapids_pytools.rapids.rapids_job import RapidsJob, RapidsLocalJob, RapidsSubmitSparkJob


@dataclass
class DataprocLocalRapidsJob(RapidsLocalJob):
    """
    Implementation of a RAPIDS job that runs on a local machine.
    """
    job_label = 'dataprocLocal'


@dataclass
class DataprocSubmitSparkRapidsJob(RapidsSubmitSparkJob):
    """
    Implementation of a RAPIDS job that runs on a remote .
    """
    job_label = 'dataprocRemoteSparkJobSubmission'


@dataclass
class DataprocServerlessRapidsJob(RapidsJob):
    """
    An implementation that uses Dataproc-Serverless to run RAPIDS accelerator tool.
    """

    def _build_submission_cmd(self):
        pass

    def _submit_job(self, cmd_args: list) -> str:
        pass
