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

import re
import os
import tempfile
import pytest  # pylint: disable=import-error
from cli_test_helpers import ArgvContext, EnvironContext  # pylint: disable=import-error

from mock_cluster import mock_live_cluster
from spark_rapids_pytools import wrapper


@patch('spark_rapids_pytools.common.utilities.SysCmd.build')
@pytest.mark.parametrize('cloud', ['dataproc', 'emr'])
def test_info_collect(build_mock, cloud, capsys):
    """Test diagnostic function about info collection."""
    return_values = mock_live_cluster[cloud]

    # Mock return values for info collection
    return_values += ['done'] * 6

    mock = Mock()
    mock.exec = Mock(side_effect=return_values)
    build_mock.return_value = mock

    with tempfile.TemporaryDirectory() as tmpdir:
        key_file = os.path.join(tmpdir, 'test.pem')

        if cloud == 'emr':
            # create empty ssh key file for EMR test
            with open(key_file, 'a', encoding='utf8') as file:
                file.close()

        with EnvironContext(RAPIDS_USER_TOOLS_KEY_PAIR_PATH=key_file):
            with ArgvContext('spark_rapids_user_tools', cloud, 'diagnostic', 'test-cluster',
                             '--output_folder', tmpdir, '--verbose'):
                wrapper.main()

    if cloud == 'dataproc':
        assert len(build_mock.call_args_list) == 13

    elif cloud == 'emr':
        assert len(build_mock.call_args_list) == 12

    _, stderr = capsys.readouterr()
    assert re.match(r".*Archive '/tmp/.*/diag_.*\.tar' is successfully created\..*", stderr, re.DOTALL)
