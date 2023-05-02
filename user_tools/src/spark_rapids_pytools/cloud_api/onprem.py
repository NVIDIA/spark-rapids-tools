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


"""Implementation specific to OnPrem"""

from dataclasses import dataclass
from typing import Any

from spark_rapids_pytools.rapids.rapids_job import RapidsLocalJob
from spark_rapids_pytools.cloud_api.sp_types import PlatformBase, CMDDriverBase, CloudPlatform
from spark_rapids_pytools.common.sys_storage import StorageDriver


@dataclass
class OnPremPlatform(PlatformBase):
    """
    Represents the interface and utilities required by OnPrem platform.
    """

    def __post_init__(self):
        self.type_id = CloudPlatform.ONPREM
        super().__post_init__()

    def _construct_cli_object(self):
        return CMDDriverBase(timeout=0, cloud_ctxt=self.ctxt)

    def _install_storage_driver(self):
        self.storage = OnPremStorageDriver(self.cli)

    def _construct_cluster_from_props(self, cluster: str, props: str = None):
        pass

    def migrate_cluster_to_gpu(self, orig_cluster):
        pass

    def create_submission_job(self, job_prop, ctxt) -> Any:
        pass

    def create_local_submission_job(self, job_prop, ctxt) -> Any:
        return RapidsLocalJob(prop_container=job_prop, exec_ctxt=ctxt)


@dataclass
class OnPremStorageDriver(StorageDriver):
    cli: CMDDriverBase
