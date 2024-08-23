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
This module defines environment setup and teardown functions for the end-to-end tests using behave.
"""

import os
import shutil
import tempfile

from spark_rapids_tools.utils import Utilities
from steps.e2e_utils import get_tools_root_path, get_e2e_tests_resource_path, get_logger, run_sys_cmd

""" Define behave hooks for the tests. These hooks are automatically called by behave. """

logger = get_logger()


def before_all(context) -> None:
    """
    Set up the environment for the tests. This function is automatically called before all the tests.
    """
    context.temp_dir = tempfile.mkdtemp()
    _set_environment_variables(context)
    _create_python_venv(context)


def after_all(context) -> None:
    """
    Clean up the environment after the tests. This function is automatically called after all the tests.
    """
    _clear_environment_variables()
    shutil.rmtree(context.temp_dir)


def before_scenario(context, scenario) -> None:
    if "skip" in scenario.effective_tags:
        scenario.skip("Marked with @skip")
        return


def after_scenario(context, scenario) -> None:
    """
    Clean up the environment after each scenario. This function is automatically called after each scenario.
    Steps must set the callback function using set_after_scenario_fn() to perform any cleanup.
    """
    if hasattr(context, 'after_scenario_fn'):
        context.after_scenario_fn()


def _set_environment_variables(context) -> None:
    """
    Set environment variables needed for the virtual environment setup.
    """
    tools_version = Utilities.get_base_release()
    scala_version = context.config.userdata.get('scala_version')
    venv_name = context.config.userdata.get('venv_name')
    jar_filename = f'rapids-4-spark-tools_{scala_version}-{tools_version}-SNAPSHOT.jar'
    build_jar_value = context.config.userdata.get('build_jar')
    build_jar = build_jar_value.lower() in ['true', '1', 'yes']

    os.environ['TOOLS_DIR'] = get_tools_root_path()
    os.environ['SCRIPTS_DIR'] = os.path.join(get_e2e_tests_resource_path(), 'scripts')
    os.environ['TOOLS_JAR_PATH'] = os.path.join(os.environ['TOOLS_DIR'], f'core/target/{jar_filename}')
    os.environ['VENV_DIR'] = os.path.join(context.temp_dir, venv_name)
    os.environ['BUILD_JAR'] = 'true' if build_jar else 'false'


def _create_python_venv(context) -> None:
    """
    Create a Python virtual environment for the tests.
    """
    script_file_name = context.config.userdata.get('setup_script_file')
    script = os.path.join(os.environ['SCRIPTS_DIR'], script_file_name)
    try:
        warning_msg = "Setting up the virtual environment for the tests. This may take a while."
        if os.environ.get('BUILD_JAR') == 'true':
            warning_msg = f'Building JAR and {warning_msg}'
        logger.warning(warning_msg)
        result = run_sys_cmd([script])
        result.check_returncode()
    except Exception as e:  # pylint: disable=broad-except
        raise RuntimeError(f"Failed to create virtual environment. Reason: {str(e)}") from e


def _clear_environment_variables() -> None:
    """
    Clear environment variables set for the virtual environment setup.
    """
    env_vars = ['SCRIPTS_DIR', 'VENV_DIR', 'TOOLS_JAR_PATH']
    for key in env_vars:
        os.environ.pop(key, None)
