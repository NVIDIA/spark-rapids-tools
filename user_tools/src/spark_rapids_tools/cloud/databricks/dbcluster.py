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
Define implementation for the databricks cluster
"""

from typing import ClassVar, Type, Optional

from spark_rapids_tools.cloud.cluster import register_client_cluster, register_cluster_prop_mgr, ClusterPropMgr, ClientCluster
from spark_rapids_tools.utils.propmanager import PropValidatorSchema


class DBAwsClusterSchema(PropValidatorSchema):
    cluster_id: str
    driver: dict
    aws_attributes: dict
    spark_conf: Optional[dict] = None


class DBAzureClusterSchema(PropValidatorSchema):
    cluster_id: str
    driver: dict
    azure_attributes: dict
    spark_conf: Optional[dict] = None


@register_cluster_prop_mgr('databricks_aws')
class DBAwsClusterPropMgr(ClusterPropMgr):
    schema_clzz: ClassVar[Type['PropValidatorSchema']] = DBAwsClusterSchema


@register_client_cluster('databricks_aws')
class DBAwsClientCluster(ClientCluster):   # pylint: disable=too-few-public-methods
    pass


@register_cluster_prop_mgr('databricks_azure')
class DBAzureClusterPropMgr(ClusterPropMgr):
    schema_clzz: ClassVar[Type['PropValidatorSchema']] = DBAzureClusterSchema


@register_client_cluster('databricks_azure')
class DBAzureClientCluster(ClientCluster):   # pylint: disable=too-few-public-methods
    pass
