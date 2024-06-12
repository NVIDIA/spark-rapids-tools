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
This module defines steps to be used by the end-to-end tests using behave.
"""

import os
import subprocess
import threading
from time import sleep
from typing import Callable

from behave import given, when, then

from e2e_utils import resolve_event_logs, create_spark_rapids_cmd, remove_cli_from_path, run_test


def set_after_scenario_fn(context, fn: Callable) -> None:
    context.after_scenario_fn = fn


@given('platform is "{platform}"')
def step_set_platform(context, platform):
    context.platform = platform


@given('"{cli}" is not installed')
def step_remove_cli_from_path(context, cli):
    original_path = os.environ["PATH"]
    remove_cli_from_path(cli)

    def after_scenario_fn():
        os.environ.update({"PATH": original_path})

    set_after_scenario_fn(context, after_scenario_fn)


def start_qualification_tool_crash_thread_helper(_stop_event: threading.Event) -> None:
    qual_tool_class_path = 'com.nvidia.spark.rapids.tool.qualification.QualificationMain'
    os.environ['LC_ALL'] = 'C'

    def is_qual_tool_running():
        result = subprocess.run(["pgrep", "-f", f"java.*{qual_tool_class_path}"], capture_output=True)
        return result.returncode == 0

    while not _stop_event.is_set() and not is_qual_tool_running():
        sleep(1)

    if is_qual_tool_running():
        sleep(1)
        subprocess.run(["pkill", "-f", f"java.*{qual_tool_class_path}"], capture_output=True)


@given('thread to crash qualification tool has started')
def step_start_qualification_tool_crash_thread(context):
    stop_event = threading.Event()
    qual_tool_thread = threading.Thread(target=start_qualification_tool_crash_thread_helper, args=(stop_event,))
    qual_tool_thread.start()

    def after_scenario_fn():
        stop_event.set()
        qual_tool_thread.join()
        stop_event.clear()

    set_after_scenario_fn(context, after_scenario_fn)


@when('spark-rapids tool is executed with "{event_logs}" eventlogs')
def step_execute_spark_rapids_tool(context, event_logs):
    event_logs = resolve_event_logs(event_logs.split(","))
    if hasattr(context, 'platform'):
        cmd = create_spark_rapids_cmd(event_logs, context.temp_dir, context.platform)
    else:
        cmd = create_spark_rapids_cmd(event_logs, context.temp_dir)
    context.result = run_test(cmd)


@then('stderr contains the following')
def step_verify_stderr(context):
    expected_stderr_list = context.text.strip().split(";")
    for stderr_line in expected_stderr_list:
        assert stderr_line in context.result.stderr, \
            f"Expected stderr line '{stderr_line}' not found. Actual stderr: {context.result.stderr}"

@then('stdout contains the following')
def step_verify_stdout(context):
    expected_stdout_list = context.text.strip().split(";")
    for stdout_line in expected_stdout_list:
        assert stdout_line in context.result.stdout, \
            f"Expected stdout line '{stdout_line}' not found. Actual stdout: {context.result.stdout}"


@then('return code is "{return_code}"')
def step_verify_return_code(context, return_code):
    assert context.result.returncode == int(return_code)
