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
This module defines steps to be used by the end-to-end tests using behave.
"""

import os
import shutil
import tempfile
import threading
from time import sleep
from typing import Callable

from behave import given, when, then

from e2e_utils import E2ETestUtils, HdfsTestUtils, HdfsStatus

logger = E2ETestUtils.get_logger()


def set_after_scenario_fn(context, fn: Callable) -> None:
    """
    Set the callback function to be called after each scenario.

    See also:
         user_tools.tests.spark_rapids_tools_e2e.features.environment.after_scenario()
    """
    context.after_scenario_fn = fn


@given('platform is "{platform}"')
def step_set_platform(context, platform) -> None:
    context.platform = platform


@given('target_cluster_info "{target_cluster_info}" is provided')
def step_set_target_cluster_info(context, target_cluster_info) -> None:
    target_cluster_info_path = os.path.join(E2ETestUtils.get_e2e_tests_resource_path(),
                                            'target_cluster_files', target_cluster_info)
    context.target_cluster_info = target_cluster_info_path


@given('"{cli}" is not installed')
def step_replace_cli_with_mock(context, cli) -> None:
    original_path = os.environ["PATH"]
    tempdir = tempfile.mkdtemp()

    E2ETestUtils.replace_cli_with_mock(cli, tempdir)

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

    def is_qual_tool_running() -> bool:
        return E2ETestUtils.run_sys_cmd(["pgrep", "-f", f"java.*{qual_tool_class_path}"]).returncode == 0

    while not _stop_event.is_set() and not is_qual_tool_running():
        sleep(1)

    if is_qual_tool_running():
        cmd_result = E2ETestUtils.run_sys_cmd(["pkill", "-f", f"java.*{qual_tool_class_path}"])
        E2ETestUtils.assert_sys_cmd_return_code(cmd_result,
                                                exp_return_code=0,
                                                error_msg="Failed to kill the qualification tool.")


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
    if HdfsTestUtils.hdfs_is_active():
        raise RuntimeError('HDFS is already active. Please stop it before running the tests.')

    test_hdfs_status = HdfsStatus.fromstring(status)
    if test_hdfs_status == HdfsStatus.NOT_INSTALLED:
        # Do nothing if HDFS should not be installed
        return
    if test_hdfs_status == HdfsStatus.RUNNING:
        # Set up HDFS and start it
        logger.warning('Setting up and starting HDFS. This may take a while.')
        should_run = True
    elif test_hdfs_status == HdfsStatus.NOT_RUNNING:
        # Set up HDFS but do not start it
        logger.warning('Setting up HDFS without starting it. This may take a while.')
        should_run = False
    else:
        raise ValueError(f"HDFS status '{status}' is not valid.")

    set_after_scenario_fn(context, HdfsTestUtils.cleanup_hdfs)
    HdfsTestUtils.setup_hdfs(should_run)


@given('HDFS has "{event_logs}" eventlogs')
def step_hdfs_has_eventlogs(context, event_logs) -> None:
    event_logs_list = E2ETestUtils.resolve_event_logs(event_logs.split(","))
    hdfs_event_logs_dir = '/'
    for event_log in event_logs_list:
        hdfs_copy_cmd = ['hdfs', 'dfs', '-copyFromLocal',  '-f', event_log, hdfs_event_logs_dir]
        cmd_result = E2ETestUtils.run_sys_cmd(hdfs_copy_cmd)
        E2ETestUtils.assert_sys_cmd_return_code(cmd_result, exp_return_code=0,
                                                error_msg="Failed to copy event logs to HDFS")


@when('"{file}" file is generated')
def step_file_is_generated(context, file) -> None:
    file_paths = {}
    local_file_path = None
    for root, _, files in os.walk(context.temp_dir):
        if file in files:
            local_file_path = os.path.join(root, file)
            # Use the file name without extension as the key
            file_key = os.path.splitext(file)[0]
            file_paths[file_key] = local_file_path
    assert local_file_path is not None, f"File '{file}' was not found in the directory '{context.temp_dir}'"
    if not hasattr(context, 'generated_file_paths'):
        context.generated_file_paths = {}
    context.generated_file_paths.update(file_paths)


@when('spark-rapids "qualification" tool is executed with "{event_logs}" eventlogs')
@when('spark-rapids tool is executed with "{event_logs}" eventlogs')  # For backward compatibility
def step_execute_spark_rapids_qual_tool(context, event_logs) -> None:
    event_logs_list = E2ETestUtils.resolve_event_logs(event_logs.split(","))
    platform = getattr(context, 'platform', 'onprem')
    target_cluster_info = getattr(context, 'target_cluster_info', None)

    cmd = E2ETestUtils.create_spark_rapids_qual_cmd(
        event_logs=event_logs_list,
        output_dir=context.temp_dir,
        platform=platform,
        target_cluster_info=target_cluster_info
    )
    context.result = E2ETestUtils.run_sys_cmd(cmd)


@when('spark-rapids "profiling" tool is executed with "{event_logs}" eventlogs')
def step_execute_spark_rapids_profiling_tool(context, event_logs) -> None:
    event_logs_list = E2ETestUtils.resolve_event_logs(event_logs.split(","))
    platform = getattr(context, 'platform', 'onprem')
    target_cluster_info = getattr(context, 'target_cluster_info', None)

    cmd = E2ETestUtils.create_spark_rapids_profiling_cmd(
        event_logs=event_logs_list,
        output_dir=context.temp_dir,
        platform=platform,
        target_cluster_info=target_cluster_info
    )
    context.result = E2ETestUtils.run_sys_cmd(cmd)


@when('"{app_id}" app is not qualified')
def step_verify_gpu_speedup_category(context, app_id) -> None:
    df = E2ETestUtils.read_csv_as_dataframe(context.generated_file_paths['qualification_summary'])
    row = df.loc[df["App ID"] == app_id]
    assert not row.empty, f'App ID "{app_id}" not found in the CSV file.'
    category = row["Estimated GPU Speedup Category"].iloc[0]
    assert category == "Not Recommended", \
        f'Expected "Not Recommended", but found "{category}"'


@then('not qualified reason is "{expected_reason}"')
def step_verify_not_qualified_reason(context, expected_reason) -> None:
    """
    Verify the 'Not Recommended Reason' column in the generated CSV file matches the expected reason.
    """
    df = E2ETestUtils.read_csv_as_dataframe(context.generated_file_paths['qualification_summary'])
    assert "Not Recommended Reason" in df.columns, "'Not Recommended Reason' column not found in the CSV file."
    actual_reason = df["Not Recommended Reason"].iloc[0]
    assert actual_reason == expected_reason, f"Expected reason: '{expected_reason}', but found: '{actual_reason}'"


@then('stderr contains the following')
def step_verify_stderr(context) -> None:
    expected_stderr_list = context.text.strip().split(";")
    for stderr_line in expected_stderr_list:
        assert stderr_line in context.result.stderr, \
            (f"Expected stderr line '{stderr_line}' not found\n" +
             E2ETestUtils.get_cmd_output_str(context.result))


@then('stdout does "{contain}" the following')
def step_verify_stdout_containment(context, contain) -> None:
    expected_stdout_list = context.text.strip().split(";")
    for stdout_line in expected_stdout_list:
        if contain == "contain":
            assert stdout_line in context.result.stdout, \
                (f"Expected stdout line '{stdout_line}' not found\n" +
                 E2ETestUtils.get_cmd_output_str(context.result))
        elif contain == "not contain":
            assert stdout_line not in context.result.stdout, \
                (f"Unexpected stdout line '{stdout_line}' found\n" +
                 E2ETestUtils.get_cmd_output_str(context.result))
        else:
            raise ValueError(f"Invalid contain value: '{contain}'. Must be 'contain' or 'not contain'")


@then('processed applications is "{expected_num_apps}"')
def step_verify_num_apps(context, expected_num_apps) -> None:
    actual_num_apps = -1
    for stdout_line in context.result.stdout.splitlines():
        if "Processed applications" in stdout_line:
            actual_num_apps = int(stdout_line.split()[-1])
    assert actual_num_apps == int(expected_num_apps), \
        f"Expected: {expected_num_apps}, Actual: {actual_num_apps}"


@then('return code is "{return_code:d}"')
def step_verify_return_code(context, return_code) -> None:
    assert context.result.returncode == return_code, \
        f"Expected return code: {return_code}, Actual return code: {context.result.returncode}"
