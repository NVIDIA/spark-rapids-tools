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


@pytest.mark.parametrize('cloud', ['dataproc', 'emr'])
class TestInfoCollect:
    """Test info collect functions."""

    def run_tool(self, cloud, args=['--verbose'], expected_exception=None):  # pylint: disable=dangerous-default-value
        with tempfile.TemporaryDirectory() as tmpdir:
            key_file = os.path.join(tmpdir, 'test.pem')

            if cloud == 'emr':
                # create empty ssh key file for EMR test
                with open(key_file, 'a', encoding='utf8') as file:
                    file.close()

            with EnvironContext(RAPIDS_USER_TOOLS_KEY_PAIR_PATH=key_file):
                with ArgvContext('spark_rapids_user_tools', cloud, 'diagnostic', 'test-cluster',
                                 '--output_folder', tmpdir, *args):
                    if expected_exception:
                        with pytest.raises(expected_exception):
                            wrapper.main()
                    else:
                        wrapper.main()

    @patch('spark_rapids_pytools.common.utilities.SysCmd.build')
    def test_info_collect(self, build_mock, cloud, capsys):
        return_values = mock_live_cluster[cloud].copy()

        # Mock return values for info collection
        return_values += ['done'] * 6

        mock = Mock()
        mock.exec = Mock(side_effect=return_values)
        build_mock.return_value = mock

        self.run_tool(cloud)

        if cloud == 'dataproc':
            assert len(build_mock.call_args_list) == 13

        elif cloud == 'emr':
            assert len(build_mock.call_args_list) == 12

        _, stderr = capsys.readouterr()
        assert re.match(r".*Archive '/tmp/.*/diag_.*\.tar' is successfully created\..*", stderr, re.DOTALL)

    @patch('spark_rapids_pytools.common.utilities.SysCmd.build')
    def test_thread_num(self, build_mock, cloud, capsys):
        return_values = mock_live_cluster[cloud].copy()

        # Mock return values for info collection
        return_values += ['done'] * 6

        mock = Mock()
        mock.exec = Mock(side_effect=return_values)
        build_mock.return_value = mock

        self.run_tool(cloud, ['--thread_num', '7', '--verbose'])

        if cloud == 'dataproc':
            assert len(build_mock.call_args_list) == 13

        elif cloud == 'emr':
            assert len(build_mock.call_args_list) == 12

        _, stderr = capsys.readouterr()

        assert 'Set thread number as: 7' in stderr
        assert re.match(r".*Archive '/tmp/.*/diag_.*\.tar' is successfully created\..*", stderr, re.DOTALL)

    @patch('spark_rapids_pytools.common.utilities.SysCmd.build')
    @pytest.mark.parametrize('thread_num', ['0', '11', '123'])
    def test_invalid_thread_num(self, build_mock, cloud, thread_num, capsys):
        return_values = mock_live_cluster[cloud].copy()

        # Mock return values for info collection
        return_values += ['done'] * 6

        mock = Mock()
        mock.exec = Mock(side_effect=return_values)
        build_mock.return_value = mock

        self.run_tool(cloud, ['--thread_num', thread_num, '--verbose'], SystemExit)

        if cloud == 'dataproc':
            assert len(build_mock.call_args_list) == 7

        elif cloud == 'emr':
            assert len(build_mock.call_args_list) == 6

        _, stderr = capsys.readouterr()

        assert 'Invalid thread number' in stderr
        assert 'Raised an error in phase [Process-Arguments]' in stderr

    @patch('spark_rapids_pytools.common.utilities.SysCmd.build')
    def test_upload_failed(self, build_mock, cloud, capsys):
        return_values = mock_live_cluster[cloud].copy()
        return_values.reverse()

        # Mock failure for upload
        def mock_exec():
            if return_values:
                return return_values.pop()

            raise RuntimeError('mock test_upload_failed')

        mock = Mock()
        mock.exec = mock_exec
        build_mock.return_value = mock

        self.run_tool(cloud, ['--thread_num', '1', '--verbose'], expected_exception=SystemExit)

        if cloud == 'dataproc':
            assert len(build_mock.call_args_list) == 8

        elif cloud == 'emr':
            assert len(build_mock.call_args_list) == 7

        _, stderr = capsys.readouterr()

        assert 'Error while uploading script to node' in stderr
        assert 'Raised an error in phase [Execution]' in stderr

    @patch('spark_rapids_pytools.common.utilities.SysCmd.build')
    def test_download_failed(self, build_mock, cloud, capsys):
        return_values = mock_live_cluster[cloud].copy()

        # Mock return values for info collection
        return_values += ['done'] * 4
        return_values.reverse()

        # Mock return values for info collection
        def mock_exec():
            if return_values:
                return return_values.pop()

            raise RuntimeError('mock test_download_failed')

        mock = Mock()
        mock.exec = mock_exec
        build_mock.return_value = mock

        self.run_tool(cloud, ['--thread_num', '1', '--verbose'], expected_exception=SystemExit)

        if cloud == 'dataproc':
            assert len(build_mock.call_args_list) == 13

        elif cloud == 'emr':
            assert len(build_mock.call_args_list) == 12

        _, stderr = capsys.readouterr()

        assert 'Error while downloading collected info from node' in stderr
        assert 'Raised an error in phase [Collecting-Results]' in stderr
