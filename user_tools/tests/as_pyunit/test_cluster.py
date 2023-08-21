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

"""Test Identifying cluster from properties"""

import pytest

from as_pytools import CspPath
from as_pytools.cloud import ClientCluster
from .conftest import AsCliUnitTest


class TestClusterCSP(AsCliUnitTest):  # pylint: disable=too-few-public-methods
    """
    Class testing identifying the cluster type by comparing the properties to
    the defined Schema
    """
    @pytest.mark.parametrize('cluster_type,cluster_prop_path', [
        ('dataproc', 'cluster/dataproc/cpu-00.yaml'),
        ('emr', 'cluster/emr/cpu-00.json'),
        ('onprem', 'cluster/onprem/cpu-00.yaml')
    ])
    def test_cluster(self, cluster_type, cluster_prop_path, get_ut_data_dir):
        client_cluster = ClientCluster(CspPath(f'{get_ut_data_dir}/{cluster_prop_path}'))
        assert client_cluster.platform_name == cluster_type
