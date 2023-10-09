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

"""
Define implementation for the dataproc cluster
"""

from typing import ClassVar, Type

from spark_rapids_tools.cloud.cluster import ClientCluster, register_client_cluster, ClusterPropMgr, register_cluster_prop_mgr
from spark_rapids_tools.utils.propmanager import PropValidatorSchemaCamel, PropValidatorSchema


class DataprocClusterSchema(PropValidatorSchemaCamel):
    cluster_name: str
    cluster_uuid: str
    project_id: str
    config: dict


class DataprocGkeClusterSchema(PropValidatorSchemaCamel):
    cluster_name: str
    cluster_uuid: str
    project_id: str
    config: dict


@register_cluster_prop_mgr('dataproc')
class DataprocClusterPropMgr(ClusterPropMgr):
    schema_clzz: ClassVar[Type[PropValidatorSchema]] = DataprocClusterSchema


@register_client_cluster('dataproc')
class DataprocClientCluster(ClientCluster):   # pylint: disable=too-few-public-methods
    pass


@register_cluster_prop_mgr('dataproc_gke')
class DataprocGkeClusterPropMgr(ClusterPropMgr):
    schema_clzz: ClassVar[Type[PropValidatorSchema]] = DataprocGkeClusterSchema


@register_client_cluster('dataproc_gke')
class DataprocGkeClientCluster(ClientCluster):   # pylint: disable=too-few-public-methods
    pass

