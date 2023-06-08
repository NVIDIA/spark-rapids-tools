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
"""Test Diagnostic functions."""

from unittest.mock import patch, Mock

import json
import re
import yaml
from cli_test_helpers import ArgvContext  # pylint: disable=import-error

from spark_rapids_pytools import wrapper


@patch('spark_rapids_pytools.common.utilities.SysCmd.build')
def test_info_collect(build_mock, capsys):
    """Test diagnostic function about info collection."""

    cluster_info = None

    with open('tests/resources/dataproc-test-gpu-cluster.yaml', 'r', encoding='utf-8') as yaml_file:
        static_properties = yaml.safe_load(yaml_file)
        cluster_info = json.dumps(static_properties)

    node_info = json.dumps({
        'guestCpus': 32,
        'memoryMb': 212992,
    })

    gpu_info = json.dumps({'description': 'NVIDIA T4'})

    # Mock return values for connect to a live cluster
    return_values = [
        'us-central1',          # gcloud config get compute/region
        'us-central1-a',        # gcloud config get compute/zone
        'dataproc-project-id',  # gcloud config get core/project
        cluster_info,
        node_info,
        gpu_info,
        node_info,
        gpu_info,
        node_info,
    ]

    # Mock return values for info collection
    return_values += ['done'] * 9

    mock = Mock()
    mock.exec = Mock(side_effect=return_values)
    build_mock.return_value = mock

    with ArgvContext('spark_rapids_user_tools', 'dataproc', 'diagnostic', 'dataproc-test-gpu-cluster',
                     '--output-folder', '/tmp'):
        wrapper.main()

    assert len(build_mock.call_args_list) == 18

    _, stderr = capsys.readouterr()
    assert re.match(r".*Archive '/tmp/diag_.*\.tar' is successfully created\..*", stderr, re.DOTALL)
