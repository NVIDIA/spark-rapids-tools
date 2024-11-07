# Copyright (c) 2024, NVIDIA CORPORATION.
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
This module defines utility functions used by the end-to-end tests using behave.
"""

import logging
import os
import subprocess
from attr import dataclass
from enum import auto
from pathlib import Path
from typing import List
from urllib.parse import urlparse

from spark_rapids_tools import EnumeratedType


@dataclass
class E2ETestUtils:

    @staticmethod
    def get_cmd_output_str(cmd_result: subprocess.CompletedProcess) -> str:
        """
        Get the output of a command as a string (stdout and stderr).
        """
        dash_line = '-' * 50
        cmd_sections = [("COMMAND", ' '.join(cmd_result.args)),
                        ("STDOUT", cmd_result.stdout),
                        ("STDERR", cmd_result.stderr)]
        output_sections = []
        for label, content in cmd_sections:
            content = content.strip() if content else "No output"
            output_sections.append(f"{dash_line}\n{label}\n{dash_line}\n{content}\n{dash_line}")
        return '\n'.join(output_sections).strip() + '\n'

    @classmethod
    def run_sys_cmd(cls, cmd: list) -> subprocess.CompletedProcess:
        """
        Run a system command and return the result.
        If verbose mode is enabled by the behave config, print the command and its output
        """
        cmd_result = subprocess.run(cmd, capture_output=True, text=True)
        if cls.is_verbose_mode():
            print(cls.get_cmd_output_str(cmd_result))
        return cmd_result

    @classmethod
    def assert_sys_cmd_return_code(cls,
                                   cmd_result: subprocess.CompletedProcess,
                                   exp_return_code: int = 0,
                                   error_msg: str = None) -> None:
        assert cmd_result.returncode == exp_return_code, \
            f"{error_msg}\n{cls.get_cmd_output_str(cmd_result)}"

    @classmethod
    def create_spark_rapids_cmd(cls,
                                event_logs: List[str],
                                output_dir: str,
                                platform: str = 'onprem',
                                filter_apps: str = 'all') -> List[str]:
        """
        Create the command to run the Spark Rapids qualification tool.
        TODO: We can add more options to the command as needed.
        """
        return [
            cls.get_spark_rapids_cli(),
            'qualification',
            '--platform', platform,
            '--eventlogs', ','.join(event_logs),
            '-o', output_dir,
            '--tools_jar', cls.get_tools_jar_file(),
            '--verbose',
            '--filter_apps', filter_apps
        ]

    # Utility getter functions
    @staticmethod
    def get_tools_root_path() -> str:
        return str(Path(__file__).parents[5])

    @staticmethod
    def get_e2e_tests_root_path() -> str:
        return str(Path(__file__).parents[2])

    @classmethod
    def get_e2e_tests_config_file(cls) -> str:
        return os.path.join(cls.get_e2e_tests_root_path(), 'behave.ini')

    @classmethod
    def get_e2e_tests_resource_path(cls) -> str:
        return os.path.join(cls.get_e2e_tests_root_path(), 'resources')

    @classmethod
    def get_local_event_logs_dir(cls) -> str:
        return os.path.join(cls.get_e2e_tests_resource_path(), 'event_logs')

    @staticmethod
    def get_spark_rapids_cli() -> str:
        return os.path.join(os.environ['E2E_TEST_VENV_DIR'], 'bin', 'spark_rapids')

    @staticmethod
    def get_tools_jar_file() -> str:
        return os.environ['E2E_TEST_TOOLS_JAR_PATH']

    @staticmethod
    def is_verbose_mode() -> bool:
        return os.environ['E2E_TEST_VERBOSE_MODE'].lower() == 'true'

    @classmethod
    def resolve_event_logs(cls, event_logs: List[str]) -> List[str]:
        """
        Get the full path of the event logs if they are local files.
        """
        # Base directory can be modified (i.e. separate for local and CICD runs)
        fs = urlparse(event_logs[0]).scheme
        if not fs or fs == 'file':
            event_logs_dir = cls.get_local_event_logs_dir()
            return [os.path.join(event_logs_dir, event_log) for event_log in event_logs]
        return event_logs

    @classmethod
    def replace_cli_with_mock(cls, cli_name: str, temp_dir: str) -> None:
        """
        Replace the specified CLI in the PATH environment variable with a mock version that simulates the
        command not being found.

        :param cli_name: The name of the CLI command to replace in the PATH.
        :param temp_dir: The temporary directory where the mock CLI will be created.
        """
        mock_cli_path = os.path.join(temp_dir, cli_name)
        with open(mock_cli_path, "w") as f:
            f.write("#!/bin/bash\n")
            f.write(f"echo '{cli_name}: command not found'\n")
            f.write("exit 1\n")
        os.chmod(mock_cli_path, 0o755)
        os.environ['PATH'] = temp_dir + ":" + os.environ['PATH']

        # verify the CLI is not in the PATH
        cmd_result = cls.run_sys_cmd([cli_name])
        cls.assert_sys_cmd_return_code(cmd_result, exp_return_code=1, error_msg=f"{cli_name} is still in the PATH")

    @staticmethod
    def get_logger() -> logging.Logger:
        """
        Create a logger for the module.
        """
        logging.basicConfig(
            level=logging.INFO,
            format='%(levelname)s: %(message)s'
        )
        return logging.getLogger(__name__)


class HdfsStatus(EnumeratedType):
    RUNNING = auto()
    NOT_RUNNING = auto()
    NOT_INSTALLED = auto()

    @classmethod
    def fromstring(cls, value: str) -> 'EnumeratedType':
        return super().fromstring(value.replace(' ', '_'))


class HdfsTestUtils:

    @staticmethod
    def setup_hdfs(should_run: bool) -> None:
        """
        Sets up the HDFS environment.

        Executes a shell script to set up HDFS and configures the environment variables
        required for HDFS. Depending on the `should_run` parameter, it either starts HDFS or simply
        configures the environment without starting it.
        :param should_run: Boolean flag to indicate whether to start HDFS.
        """
        try:
            hdfs_setup_script = os.path.join(os.environ['E2E_TEST_SCRIPTS_DIR'], 'hdfs', 'setup_hdfs.sh')
            args = ["--run" if should_run else "--no-run"]
            cmd_result = E2ETestUtils.run_sys_cmd([hdfs_setup_script] + args)
            E2ETestUtils.assert_sys_cmd_return_code(cmd_result, exp_return_code=0, error_msg="Failed to setup HDFS")
            hadoop_home = cmd_result.stdout.splitlines()[-1]
            hadoop_conf_dir = os.path.join(hadoop_home, 'etc', 'hadoop')
            assert os.path.exists(hadoop_home), f"HADOOP_HOME: {hadoop_home} does not exist"
            os.environ['HADOOP_HOME'] = hadoop_home
            if not should_run:
                os.environ['HADOOP_CONF_DIR'] = hadoop_conf_dir
            os.environ['PATH'] = f"{hadoop_home}/bin:{hadoop_home}/sbin:{os.environ['PATH']}"
        except Exception as e:  # pylint: disable=broad-except
            raise RuntimeError(f"Failed to setup HDFS.\nReason: {e}") from e

    @staticmethod
    def cleanup_hdfs() -> None:
        """
        Stops the HDFS and cleans up the environment.
        """
        hdfs_cleanup_script = os.path.join(os.environ['E2E_TEST_SCRIPTS_DIR'], 'hdfs', 'cleanup_hdfs.sh')
        try:
            cmd_result = E2ETestUtils.run_sys_cmd([hdfs_cleanup_script])
            E2ETestUtils.assert_sys_cmd_return_code(cmd_result, exp_return_code=0, error_msg="Failed to stop HDFS")
        except Exception as e:  # pylint: disable=broad-except
            raise RuntimeError(f"Failed to stop HDFS.\nReason: {e}") from e

    @staticmethod
    def hdfs_is_active() -> bool:
        """
        Check if HDFS is already active.
        """
        try:
            output = subprocess.check_output(['jps'], text=True)
            return 'NameNode' in output
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            raise RuntimeError("Failed to check if HDFS is running.") from e
