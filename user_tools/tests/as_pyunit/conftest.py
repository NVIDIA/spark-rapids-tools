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

"""Add common helpers and utilities for unit-tests"""

import sys

import pytest   # pylint: disable=import-error


def get_test_resources_path():
    # pylint: disable=import-outside-toplevel
    if sys.version_info < (3, 9):
        import importlib_resources
    else:
        import importlib.resources as importlib_resources
    pkg = importlib_resources.files('tests.as_pyunit')
    return pkg / 'resources'


def gen_cpu_cluster_props():
    return [
        ('dataproc', 'cluster/dataproc/cpu-00.yaml'),
        ('emr', 'cluster/emr/cpu-00.json'),
        ('onprem', 'cluster/onprem/cpu-00.yaml'),
        ('databricks_aws', 'cluster/databricks/aws-cpu-00.json'),
        ('databricks_azure', 'cluster/databricks/azure-cpu-00.json')
    ]


all_cpu_cluster_props = gen_cpu_cluster_props()
all_csps = ['dataproc', 'emr', 'onprem', 'databricks_aws', 'databricks_azure']


class AsCliUnitTest:   # pylint: disable=too-few-public-methods

    @pytest.fixture(autouse=True)
    def get_ut_data_dir(self):
        # TODO: find a dynamic way to load the package name, instead of having it hardcoded
        return get_test_resources_path()
