# Copyright (c) 2024-2025, NVIDIA CORPORATION.
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
from glob import glob

from steps.e2e_utils import E2ETestUtils

""" Define behave hooks for the tests. These hooks are automatically called by behave. """

logger = E2ETestUtils.get_logger()


def before_all(context) -> None:
    """
    Set up the environment for the tests. This function is automatically called before all the tests.
    """
    context.temp_dir = tempfile.mkdtemp()
    _set_environment_variables(context)
    _set_verbose_mode(context)


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

    # Restore original QUALX_LABEL if it existed
    if hasattr(context, 'original_qualx_label'):
        if context.original_qualx_label is not None:
            os.environ['QUALX_LABEL'] = context.original_qualx_label
        else:
            os.environ.pop('QUALX_LABEL', None)

    # Cleaning up the output from previous scenarios if any
    if hasattr(context, 'temp_dir') and os.path.exists(context.temp_dir):
        for item_path in glob(os.path.join(context.temp_dir, 'qual_*')):
            if os.path.isdir(item_path):
                shutil.rmtree(item_path)


def _set_verbose_mode(context) -> None:
    verbose_enabled = getattr(context.config, 'verbose', False)
    if verbose_enabled:
        context.config.stdout_capture = False
        context.config.stderr_capture = False
    os.environ['E2E_TEST_VERBOSE_MODE'] = str(verbose_enabled).lower()


def _set_environment_variables(context) -> None:
    # Always resolve scripts dir via utils (independent from behave user-data)
    os.environ['E2E_TEST_SCRIPTS_DIR'] = os.path.join(E2ETestUtils.get_e2e_tests_resource_path(), 'scripts')
    os.environ.setdefault('E2E_TEST_HADOOP_VERSION', '3.3.6')
    os.environ.setdefault('E2E_TEST_TMP_DIR', '/tmp/spark_rapids_tools_e2e_tests')


def _clear_environment_variables() -> None:
    """
    Clear environment variables set for the virtual environment setup.
    """
    env_vars = ['SCRIPTS_DIR', 'VENV_DIR', 'TOOLS_JAR_PATH']
    for key in env_vars:
        os.environ.pop(key, None)
