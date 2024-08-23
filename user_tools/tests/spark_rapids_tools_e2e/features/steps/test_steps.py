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
import shutil
import subprocess
import tempfile
import threading
from time import sleep
from typing import Callable

from behave import given, when, then

from e2e_utils import resolve_event_logs, create_spark_rapids_cmd, replace_cli_with_mock, run_sys_cmd, setup_hdfs, cleanup_hdfs, get_logger

logger = get_logger()


def set_after_scenario_fn(context, fn: Callable) -> None:
    context.after_scenario_fn = fn


@given('platform is "{platform}"')
def step_set_platform(context, platform) -> None:
    context.platform = platform


@given('"{cli}" is not installed')
def step_replace_cli_with_mock(context, cli) -> None:
    original_path = os.environ["PATH"]
    tempdir = tempfile.mkdtemp()

    replace_cli_with_mock(cli, tempdir)

    def after_scenario_fn():
        os.environ.update({"PATH": original_path})
        shutil.rmtree(tempdir)

    set_after_scenario_fn(context, after_scenario_fn)


def _start_qualification_tool_crash_thread_internal(_stop_event: threading.Event) -> None:
    """
    Start a thread to crash the qualification tool after 1s of it starting.
    :param _stop_event: Event to stop the thread
    """
    qual_tool_class_path = 'com.nvidia.spark.rapids.tool.qualification.QualificationMain'
    os.environ['LC_ALL'] = 'C'

    def is_qual_tool_running() -> bool:
        return run_sys_cmd(["pgrep", "-f", f"java.*{qual_tool_class_path}"]).returncode == 0

    while not _stop_event.is_set() and not is_qual_tool_running():
        sleep(1)

    if is_qual_tool_running():
        sleep(1)
        result = run_sys_cmd(["pkill", "-f", f"java.*{qual_tool_class_path}"])
        assert result.returncode == 0, f"Failed to kill the qualification tool. Error: {result.stderr}"


@given('thread to crash qualification tool has started')
def step_start_qualification_tool_crash_thread(context) -> None:
    stop_event = threading.Event()
    qual_tool_thread = threading.Thread(target=_start_qualification_tool_crash_thread_internal, args=(stop_event,))
    qual_tool_thread.start()

    def after_scenario_fn():
        stop_event.set()
        qual_tool_thread.join()
        stop_event.clear()

    set_after_scenario_fn(context, after_scenario_fn)


@given('HDFS is "{status}"')
def step_setup_hdfs(context, status) -> None:
    logger.warning("Setting up HDFS. This may take a while.")
    cleanup_hdfs()
    set_after_scenario_fn(context, cleanup_hdfs)
    should_run = status.lower() == 'running'
    setup_hdfs(should_run)


@given('HDFS has "{event_logs}" eventlogs')
def step_hdfs_has_eventlogs(context, event_logs) -> None:
    event_logs_list = resolve_event_logs(event_logs.split(","))
    hdfs_event_logs_dir = '/'
    for event_log in event_logs_list:
        hdfs_copy_cmd = ['hdfs', 'dfs', '-copyFromLocal',  '-f', event_log, hdfs_event_logs_dir]
        hdfs_copy_status = run_sys_cmd(hdfs_copy_cmd)
        assert hdfs_copy_status.returncode == 0, f"Failed to copy event logs to HDFS. Error: {hdfs_copy_status.stderr}"


@when('spark-rapids tool is executed with "{event_logs}" eventlogs')
def step_execute_spark_rapids_tool(context, event_logs) -> None:
    event_logs_list = resolve_event_logs(event_logs.split(","))
    if hasattr(context, 'platform'):
        cmd = create_spark_rapids_cmd(event_logs_list, context.temp_dir, context.platform)
    else:
        cmd = create_spark_rapids_cmd(event_logs_list, context.temp_dir)
    context.result = run_sys_cmd(cmd)


@then('stderr contains the following')
def step_verify_stderr(context) -> None:
    expected_stderr_list = context.text.strip().split(";")
    for stderr_line in expected_stderr_list:
        assert stderr_line in context.result.stderr, \
            f"Expected stderr line '{stderr_line}' not found. Actual stderr: {context.result.stderr}"


@then('stdout contains the following')
def step_verify_stdout(context) -> None:
    expected_stdout_list = context.text.strip().split(";")
    for stdout_line in expected_stdout_list:
        assert stdout_line in context.result.stdout, \
            f"Expected stdout line '{stdout_line}' not found. Actual stdout: {context.result.stdout}"


@then('processed applications is "{expected_num_apps}"')
def step_verify_num_apps(context, expected_num_apps) -> None:
    actual_num_apps = -1
    for stdout_line in context.result.stdout.splitlines():
        if "Processed applications" in stdout_line:
            actual_num_apps = int(stdout_line.split()[-1])
    assert actual_num_apps == int(expected_num_apps), \
        f"Expected: {expected_num_apps}, Actual: {actual_num_apps}"


@then('return code is "{return_code}"')
def step_verify_return_code(context, return_code) -> None:
    assert context.result.returncode == int(return_code)
