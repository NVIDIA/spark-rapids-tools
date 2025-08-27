# Copyright (c) 2025, NVIDIA CORPORATION.
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

"""Includes the steps to test the ResultHandler."""

import os
from behave import given, when, then

from spark_rapids_tools.api_v1.builder import APIResultHandler, CSVReport, CSVReportCombiner


@given('an empty tools_out_dir "{tools_out_dir}"')
def step_given_empty_test_directory(context, tools_out_dir):
    # Using Behave's context to store state
    context.tools_out_dir_arg = os.path.join(context.temp_dir, tools_out_dir)


@when('I build a wrapper with report_id "{report_id}"')
def step_when_build_handler(context, report_id):
    context.tools_res_handler = APIResultHandler().report(report_id).with_path(context.tools_out_dir_arg).build()


@then('the result handler should be empty')
def step_then_handler_empty(context):
    is_empty_flag = context.tools_res_handler.is_empty()
    assert is_empty_flag


@when('I attempt to load a per-app CSV report "{tbl_name}"')
def step_when_load_per_app_csv_report(context, tbl_name):
    if not hasattr(context, 'csv_rep_res'):
        context.csv_rep_res = {}
    context.csv_rep_res[tbl_name] = CSVReport(context.tools_res_handler).table(tbl_name).load()


@then('CSV report result of "{tbl_name}" should be an empty dictionary')
def step_then_rep_res_is_empty_dict(context, tbl_name):
    if not hasattr(context, 'csv_rep_res'):
        context.csv_rep_res = {}
    context.csv_rep_res[tbl_name] = CSVReport(context.tools_res_handler).table(tbl_name).load()
    assert context.csv_rep_res[tbl_name] == {}, 'CSV report result should be an empty dictionary'


@then('CSV report result of status-report should fail with exception "{exception_type}"')
def step_then_rep_res_fails_with_exception(context, exception_type):
    tbl_name = 'coreCSVStatus'
    if context.tools_res_handler.report_id in ['profWrapperOutput', 'profCoreOutput']:
        tbl_name = 'coreCSVStatus'
    if not hasattr(context, 'csv_rep_res'):
        context.csv_rep_res = {}
    csv_rep = CSVReport(context.tools_res_handler).table(tbl_name)
    context.csv_rep_res[tbl_name] = csv_rep.load()
    if csv_rep.is_per_app_tbl:
        assert False, 'Expected a non-per-app table to raise an exception'
    try:
        raise context.csv_rep_res[tbl_name].get_fail_cause()
    except Exception as e:  # pylint: disable=broad-except
        assert e.__class__.__name__ == exception_type, 'The CSV should fail with FileNotFound Exception'


@then('CSV report result of "{tbl_name}" on appID "{app_id}" should fail with "{exception_type}"')
def step_then_rep_res_on_app_fails_with_exception(context, tbl_name, app_id, exception_type):
    if not hasattr(context, 'csv_rep_res'):
        context.csv_rep_res = {}
    csv_rep = CSVReport(context.tools_res_handler).table(tbl_name).app(app_id)
    csv_rep_res = csv_rep.load()
    context.csv_rep_res[tbl_name] = csv_rep_res
    assert csv_rep_res.data is None
    assert csv_rep_res.success is False, 'CSV report result should not be successful'
    assert csv_rep_res.fallen_back is False, 'CSV report result should not have fallen back'
    try:
        raise csv_rep_res.get_fail_cause()
    except Exception as e:  # pylint: disable=broad-except
        assert e.__class__.__name__ == exception_type
        assert f'Application [{app_id}] not found in report [coreRawMetrics].' in str(e)


@then('combined results of CSV report "{tbl_name}" should be empty')
def step_then_combined_csv_report_is_empty(context, tbl_name):
    if not hasattr(context, 'csv_rep_res'):
        context.csv_rep_res = {}
    csv_combined = CSVReportCombiner([
        CSVReport(context.tools_res_handler).table(tbl_name)
    ]).build()
    assert csv_combined.success
    assert csv_combined.fallen_back is False
    assert csv_combined.data.empty
