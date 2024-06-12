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

import os
import subprocess
from pathlib import Path
from typing import List


def run_test(cmd) -> subprocess.CompletedProcess:
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
    Get the full path of the event logs.
    """
    # Base directory can be modified (i.e. separate for local and CICD runs)
    event_logs_dir = get_local_event_logs_dir()
    return [os.path.join(event_logs_dir, event_log) for event_log in event_logs]


def remove_cli_from_path(cli: str) -> None:
    """
    Update the PATH environment variable to remove directories containing the specified CLI.

    This function iterates over the directories in the PATH environment variable and removes the directory containing
    the specified CLI. If the CLI is not found in the directory, the directory is kept in the PATH.

    :param cli: CLI to remove from the PATH.
    """
    paths = os.environ["PATH"].split(":")
    modified_paths = []
    for path in paths:
        # Check if the CLI is in the current path
        if os.path.basename(path) == cli and os.access(path, os.X_OK):
            continue
        # Check if the parent directory of the CLI is in the current path
        cli_path = os.path.join(path, cli)
        if os.path.exists(cli_path) and os.access(cli_path, os.X_OK):
            continue
        modified_paths.append(path)

    os.environ["PATH"] = ":".join(modified_paths)
