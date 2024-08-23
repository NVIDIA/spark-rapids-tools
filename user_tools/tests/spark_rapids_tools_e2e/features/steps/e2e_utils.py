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
from pathlib import Path
from typing import List
from urllib.parse import urlparse


def run_sys_cmd(cmd: list) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True)


def create_spark_rapids_cmd(event_logs: List[str],
                            output_dir: str,
                            platform: str = 'onprem',
                            filter_apps: str = 'all') -> List[str]:
    """
    Create the command to run the Spark Rapids qualification tool.
    """
    return [
        get_spark_rapids_cli(),
        'qualification',
        '--platform', platform,
        '--eventlogs', ','.join(event_logs),
        '-o', output_dir,
        '--tools_jar', get_tools_jar_file(),
        '--verbose',
        '--filter_apps', filter_apps
    ]


# Utility getter functions

def get_tools_root_path() -> str:
    return str(Path(__file__).parents[5])


def get_e2e_tests_root_path() -> str:
    return str(Path(__file__).parents[2])


def get_e2e_tests_config_file() -> str:
    return os.path.join(get_e2e_tests_root_path(), 'behave.ini')


def get_e2e_tests_resource_path() -> str:
    return os.path.join(get_e2e_tests_root_path(), 'resources')


def get_local_event_logs_dir() -> str:
    return os.path.join(get_e2e_tests_resource_path(), 'event_logs')


def get_spark_rapids_cli() -> str:
    return os.path.join(os.environ['VENV_DIR'], 'bin', 'spark_rapids')


def get_tools_jar_file() -> str:
    return os.environ['TOOLS_JAR_PATH']


def resolve_event_logs(event_logs: List[str]) -> List[str]:
    """
    Get the full path of the event logs if they are local files.
    """
    # Base directory can be modified (i.e. separate for local and CICD runs)
    fs = urlparse(event_logs[0]).scheme
    if not fs or fs == 'file':
        event_logs_dir = get_local_event_logs_dir()
        return [os.path.join(event_logs_dir, event_log) for event_log in event_logs]
    return event_logs


def replace_cli_with_mock(cli_name: str, temp_dir: str) -> None:
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
    assert run_sys_cmd([cli_name]).returncode != 0, f"{cli_name} is still in the PATH"


def setup_hdfs(should_run: bool) -> None:
    """
    Sets up the Hadoop Distributed File System (HDFS) environment.

    Executes a shell script to set up HDFS and configures the environment variables
    required for HDFS. Depending on the `should_run` parameter, it either starts HDFS or simply
    configures the environment without starting it.
    :param should_run: Boolean flag to indicate whether to start HDFS.
    :return:
    """
    try:
        hdfs_setup_script = os.path.join(os.environ['SCRIPTS_DIR'], 'hdfs', 'setup_hdfs.sh')
        hdfs_report_result = run_sys_cmd([hdfs_setup_script, str(should_run)])
        assert hdfs_report_result.returncode == 0, \
            f"Failed to start HDFS. \nstderr: {hdfs_report_result.stderr}\n\nstdout: {hdfs_report_result.stdout}"
        hadoop_home = hdfs_report_result.stdout.splitlines()[-1]
        hadoop_conf_dir = os.path.join(hadoop_home, 'etc', 'hadoop')
        assert os.path.exists(hadoop_home), f"HADOOP_HOME: {hadoop_home} does not exist"
        os.environ['HADOOP_HOME'] = hadoop_home
        if not should_run:
            os.environ['HADOOP_CONF_DIR'] = hadoop_conf_dir
        os.environ['PATH'] = f"{hadoop_home}/bin:{hadoop_home}/sbin:{os.environ['PATH']}"
    except Exception as e:  # pylint: disable=broad-except
        raise RuntimeError(f"Failed to start HDFS.\nReason: {e}") from e


def cleanup_hdfs() -> None:
    """
    Stops the Hadoop Distributed File System (HDFS) and cleans up the environment.
    """
    hdfs_cleanup_script = os.path.join(os.environ['SCRIPTS_DIR'], 'hdfs', 'cleanup_hdfs.sh')
    try:
        hdfs_cleanup_result = run_sys_cmd([hdfs_cleanup_script])
        assert hdfs_cleanup_result.returncode == 0, \
            f"Failed to stop HDFS.\nstderr: {hdfs_cleanup_result.stderr}\n\nstdout: {hdfs_cleanup_result.stdout}"
    except Exception as e:  # pylint: disable=broad-except
        raise RuntimeError(f"Failed to stop HDFS.\nReason: {e}") from e


def get_logger() -> logging.Logger:
    """
    Create a logger for the module.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s: %(message)s'
    )
    return logging.getLogger(__name__)
