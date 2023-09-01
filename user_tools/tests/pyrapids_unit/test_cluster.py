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

import pytest  # pylint: disable=import-error

from pyrapids import CspPath
from pyrapids.cloud import ClientCluster
from pyrapids.exceptions import InvalidPropertiesSchema
from .conftest import PyrapidsUnitTest, all_cpu_cluster_props


class TestClusterCSP(PyrapidsUnitTest):  # pylint: disable=too-few-public-methods
    """
    Class testing identifying the cluster type by comparing the properties to
    the defined Schema
    """
    def test_cluster_invalid_path(self, get_ut_data_dir):
        with pytest.raises(InvalidPropertiesSchema) as ex_schema:
            ClientCluster(CspPath(f'{get_ut_data_dir}/non_existing_file.json'))
        assert 'Incorrect properties files:' in ex_schema.value.message

    @pytest.mark.parametrize('csp,prop_path', all_cpu_cluster_props)
    def test_define_cluster_type_from_schema(self, csp, prop_path, get_ut_data_dir):
        client_cluster = ClientCluster(CspPath(f'{get_ut_data_dir}/{prop_path}'))
        assert client_cluster.platform_name == csp
