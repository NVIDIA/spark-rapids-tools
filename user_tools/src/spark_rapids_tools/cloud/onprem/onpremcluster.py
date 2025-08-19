# Copyright (c) 2023-2025, NVIDIA CORPORATION.
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

"""
Define implementation for the onPrem cluster
"""

from typing import ClassVar, Type
from pydantic import BaseModel, ConfigDict
from spark_rapids_tools.cloud.cluster import ClientCluster, ClusterPropMgr, register_cluster_prop_mgr, register_client_cluster
from spark_rapids_tools.utils.propmanager import PropValidatorSchema
from spark_rapids_tools.utils.util import to_camel_case


class OnPremDriverConfigSchema(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel_case)
    num_cores: int
    memory: str


class OnPremExecutorConfigSchema(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel_case)
    num_cores: int
    memory: str
    num_workers: int


class OnPremClusterConfigSchema(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel_case)
    master_config: OnPremDriverConfigSchema
    worker_config: OnPremExecutorConfigSchema


class OnPremClusterSchema(PropValidatorSchema):
    config: OnPremClusterConfigSchema


@register_cluster_prop_mgr('onprem')
class OnPremClusterPropMgr(ClusterPropMgr):
    schema_clzz: ClassVar[Type['PropValidatorSchema']] = OnPremClusterSchema


@register_client_cluster('onprem')
class OnPremClientCluster(ClientCluster):   # pylint: disable=too-few-public-methods
    pass
