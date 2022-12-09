# Copyright (c) 2022, NVIDIA CORPORATION.
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

"""Test Qualification functions."""

import os
import re
import subprocess
import tarfile
from unittest.mock import patch, call

import pytest  # pylint: disable=import-error
from cli_test_helpers import ArgvContext  # pylint: disable=import-error

from conftest import mock_cluster_props, get_wrapper_work_dir
from spark_rapids_dataproc_tools import dataproc_wrapper
from spark_rapids_dataproc_tools.dataproc_utils import CMDRunner
from spark_rapids_dataproc_tools.rapids_models import Qualification


@pytest.fixture(name='qual_output_dir')
def fixture_qual_output_dir(tmp_path):
    """
    Placeholder to prepare the output directory passed to the Qualification command.
    Note that there is no need to create the parent directory because the wrapper internally does that.
    """
    test_output_folder = tmp_path / 'qual'
    return test_output_folder


def qual_result_dir(root_out_directory):
    return get_wrapper_work_dir('qualification', root_out_directory)


def test_qual_failure_non_existing_cluster(capfd, qual_output_dir):
    """Test Qualification with non-existing cluster."""

    # Expected output running on invalid cluster
    # Failure Running Rapids Tool (Quallification).
    # \tCould not pull Cluster description region:us-central1, dataproc-test-cluster
    # \tRun Terminated with error.
    # \tError invoking CMD <gcloud dataproc clusters describe dataproc-test-cluster --region=us-central1>:
    # \t| ERROR: (gcloud.dataproc.clusters.describe) NOT_FOUND: Not found: \
    # Cluster projects/rapids-spark/regions/us-central1/clusters/dataproc-test-cluster
    def check_failure_content(actual_captured: str) -> bool:
        main_key_stmts = [
            'Failure Running Rapids Tool (Qualification).',
            'Could not pull Cluster description',
            'ERROR: (gcloud.dataproc.clusters.describe) NOT_FOUND: Not found:'
        ]
        return all(actual_captured.find(stmt) != -1 for stmt in main_key_stmts)

    cluster_name = 'dataproc-test-non-existing-cluster'
    # Run the actual test
    qual_args = [
        '--region=us-central1',
        f'--cluster={cluster_name}',
        f'--output_folder={qual_output_dir}'
    ]
    wrapper_args = ' '.join(qual_args)
    # pylint: disable=subprocess-run-check
    c = subprocess.run(f'spark_rapids_dataproc qualification {wrapper_args}', shell=True, text=True)
    # pylint: enable=subprocess-run-check
    captured_output = capfd.readouterr().out
    assert check_failure_content(captured_output)
    assert c.returncode != 0
    assert not os.path.exists(f'{qual_result_dir(qual_output_dir)}')


def mock_passive_copy(*unused_argv, **unused_kwargs):
    """Mock CMDRunner gcloud_cp. This is an empty implementation that does not have any side effects"""
    # argv[] ->  {qual_output_dir}/wrapper-output/rapids_user_tools_qualification
    # argv[] ->  gs://{TEMP_BUCKET}/{CLUSTER_UUID}/rapids_user_tools_qualification_{UNIQUE_ID}
    return ''


def mock_remote_download(*unused_argv, **unused_kwargs):
    """
    Mock CMDRunner gcloud_cp. This implementation simulates downloading the tool's output directory
    from gstorage to local directory.
    """
    if len(unused_argv) >= 2:
        if unused_argv[0].startswith('gs://'):
            # this is a download cmd. Trigger a local copy simulation.
            offline_files_path = 'tests/resources/output_samples/qual_success_01/tool_output_files.tar.gz'
            destination_path = f'{unused_argv[1]}/qual-tool-output/'
            with tarfile.open(offline_files_path, mode='r:*') as tar:
                tar.extractall(destination_path)
                tar.close()
    return ''


def mock_successful_run_as_spark(*unused_argv, **unused_kwargs):
    """
    Do nothing to mock that the tool is currently running
    """
    return ''


def test_qual_failure_on_spark2(capsys, caplog, qual_output_dir):
    """
    Tools cannot run on Spark2.x clusters.
    The wrapper should fail and show meaningful output to the user.
    """
    # 2022-12-07 11:14:12,582 WARNING qualification: The cluster image 1.5.75-debian10 is not supported. \
    # To support the RAPIDS user tools, you will need to use an image that runs Spark3.x.
    # Failure Running Rapids Tool (Qualification).
    #         Tool cannot execute on the execution cluster.
    #         Run Terminated with error.
    #         The cluster image 1.5.75-debian10 is not supported. To support the RAPIDS user tools, \
    #         you will need to use an image that runs Spark3.x.
    cluster_name = 'dataproc-test-spark2-cluster'

    log_key_stmts = [
        'To support the RAPIDS user tools, you will need to use an image that runs Spark3.x.'
    ]
    stdout_key_stmts = [
        'Failure Running Rapids Tool (Qualification).',
        'Tool cannot execute on the execution cluster.'
    ]
    stdout_key_stmts.extend(log_key_stmts)

    with patch.object(CMDRunner, 'gcloud_describe_cluster',
                      side_effect=[mock_cluster_props(f'{cluster_name}')]) as pull_cluster_props:
        with pytest.raises(SystemExit):
            with ArgvContext('spark_rapids_dataproc',
                             'qualification',
                             '--cluster', f'{cluster_name}',
                             '--region', 'us-central1',
                             '--output_folder', f'{qual_output_dir}'):
                dataproc_wrapper.main()
    captured_output = capsys.readouterr().out
    captured_log = caplog.text
    # assert the output message
    assert (captured_log.find(log_msg) != -1 for log_msg in log_key_stmts)
    assert (captured_output.find(stdout_msg) != -1 for stdout_msg in stdout_key_stmts)
    pull_cluster_props.assert_called_once()
    expected_pull_cluster_args = [
        call(f'{cluster_name}',
             'us-central1',
             f'Could not pull Cluster description region:us-central1, {cluster_name}')]
    assert pull_cluster_props.call_args_list == expected_pull_cluster_args
    assert not os.path.exists(f'{qual_result_dir(qual_output_dir)}')


@pytest.mark.parametrize('submission_cluster', ['dataproc-test-gpu-cluster', 'dataproc-test-nongpu-cluster'])
def test_qual_failure_non_running_cluster(capsys, qual_output_dir, submission_cluster):
    """
    Qualification runs on non GPU cluster as well as GPU cluster.
    This unit test verifies the error due to run qualification tool for an existing cluster; but it is not
    started at the time of submission.
    """
    # pylint: disable=line-too-long

    # spark_rapids_dataproc qualification --region=us-central1 --cluster=CLUSTER_NAME \
    # --output_folder={qual_output_dir}
    # 2022-12-07 12:43:12,442 INFO qualification: The original CPU cluster is the same as the submission cluster on \
    # which the tool runs. To update the configuration of the CPU cluster, make sure to pass the properties file to \
    # the CLI arguments.
    # 2022-12-07 12:43:12,443 INFO qualification: The GPU cluster is the same as the submission cluster on which the \
    # RAPIDS tool is running [ahussein-dp-spark3]. To update the configuration of the GPU cluster, \
    # make sure to pass the properties file to the CLI arguments.
    # 2022-12-07 12:43:14,079 INFO qualification: Preparing remote work env
    # 2022-12-07 12:43:15,847 INFO qualification: Upload dependencies to remote cluster
    # 2022-12-07 12:43:17,878 INFO qualification: Executing the tool
    # 2022-12-07 12:43:17,878 INFO qualification: Running the tool as a spark job on dataproc
    # Failure Running Rapids Tool (Qualification).
    #         Failed Submitting Spark job
    #         Run Terminated with error.
    #         Error invoking CMD <gcloud dataproc jobs submit spark --cluster=CLUSTER_NAME \
    #         --region=us-central1 \
    #         --jars=gs:///rapids-4-spark-tools_2.12-22.10.0.jar \
    #         --class=com.nvidia.spark.rapids.tool.qualification.QualificationMain --  \
    #         --output-directory  gs:///qual-tool-output:
    #         | ERROR: (gcloud.dataproc.jobs.submit.spark) FAILED_PRECONDITION: Unable to submit job,
    #         cluster 'CLUSTER_NAME' is in state STOPPED and cannot accept jobs.

    # pylint: enable=line-too-long

    cluster_name = submission_cluster

    log_reg_expressions = [
        (r'INFO qualification: The original CPU cluster is the same as the submission cluster on which the tool runs\. '
         r'To update the configuration of the CPU cluster, make sure to pass the properties file to the CLI arguments'),
        (r'INFO qualification: The GPU cluster is the same as the submission cluster on which the RAPIDS tool is '
         rf'running \[{cluster_name}\]\. To update the configuration of the GPU cluster, make sure to pass the '
         r'properties file to the CLI arguments\.'),
    ]
    std_reg_expressions = [
        r'Failure Running Rapids Tool \(Qualification\)\.',
        r'Failed Submitting Spark job',
        r'Run Terminated with error\.',
        r'ERROR: \(gcloud\.dataproc\.jobs\.submit\.spark\) ',
    ]
    expected_pull_cluster_args = [
        call(f'{cluster_name}',
             'us-central1',
             f'Could not pull Cluster description region:us-central1, {cluster_name}')]
    with patch.object(CMDRunner, 'gcloud_describe_cluster',
                      side_effect=[mock_cluster_props(f'{cluster_name}')]) as pull_cluster_props, \
            patch.object(CMDRunner, 'gcloud_cp',
                         side_effect=mock_passive_copy):
        with pytest.raises(SystemExit):
            with ArgvContext('spark_rapids_dataproc',
                             'qualification',
                             '--cluster', f'{cluster_name}',
                             '--region', 'us-central1',
                             '--output_folder', f'{qual_output_dir}'):
                dataproc_wrapper.main()
    captured_output = capsys.readouterr()
    assert pull_cluster_props.call_args_list == expected_pull_cluster_args
    for stdout_reg in std_reg_expressions:
        assert re.search(stdout_reg, captured_output.out), f'Could not match {stdout_reg} in tool output'
    for log_reg in log_reg_expressions:
        assert re.search(log_reg, captured_output.err), f'Could not match {log_reg} in tool log'
    assert not os.path.exists(f'{qual_result_dir(qual_output_dir)}')


@pytest.mark.parametrize('submission_cluster', ['dataproc-test-gpu-cluster', 'dataproc-test-nongpu-cluster'])
@pytest.mark.parametrize('events_scenario',
                         [{'label': 'with_events', 'mockFunc': mock_remote_download},
                          {'label': 'no_events', 'mockFunc': mock_passive_copy}])
def test_qual_success(capsys, qual_output_dir, submission_cluster, events_scenario):
    """
    This is a successful run achieved by capturing cloud_copy command. It is executed with two different scenarios:
    1- with empty eventlogs, and 2- with mocked eventlogs loaded from resources folder.
    For more details on the sample see resources/output_samples/generation.md
    :param capsys: captures the stdout of the execution.
    :param qual_output_dir: the temp folder passed as an argument to the wrapper command.
    :param submission_cluster: the name of the cluster used to submit the tool.
    :param events_scenario: the method to be used as side effect for cloud copy command.
    """

    # If the Qualification tool generates no output, it is expected to get the following output
    # Configuration Incompatibilities:
    # --------------- --------------------------------------------------------------------------------------------------
    # workerLocalSSDs - Worker nodes have no local SSDs. Local SSD is recommended for Spark scratch space to improve IO.
    # --------------- --------------------------------------------------------------------------------------------------
    # The Qualification tool did not generate any output. Nothing to display.

    cluster_name = submission_cluster
    scenario_id = events_scenario.get('label')
    wrapper_output_folder = f'{qual_result_dir(qual_output_dir)}/qual-tool-output'
    wrapper_summary_file = f'{wrapper_output_folder}/rapids_4_dataproc_qualification_output.csv'
    rapids_qual_folder = f'{wrapper_output_folder}/rapids_4_spark_qualification_output'
    log_reg_expressions = [
        (r'INFO qualification: The original CPU cluster is the same as the submission cluster on which the tool runs\. '
         r'To update the configuration of the CPU cluster, make sure to pass the properties file to the CLI arguments'),
        (r'INFO qualification: The GPU cluster is the same as the submission cluster on which the RAPIDS tool is '
         rf'running \[{cluster_name}\]\. To update the configuration of the GPU cluster, make sure to pass the '
         r'properties file to the CLI arguments\.'),
    ]
    std_reg_expressions = {
        'no_events': [
            r'Configuration Incompatibilities:',
            (r'workerLocalSSDs  '
             r'- Worker nodes have no local SSDs\. Local SSD is recommended for Spark scratch space to improve IO\.'),
            r'The Qualification tool did not generate any output\. Nothing to display\.'
        ],
        'with_events': [
            rf'Qualification tool output is saved to local disk {rapids_qual_folder}',
            rf'Full savings and speedups CSV report: {wrapper_summary_file}',
            r'Report Summary:',
            r'(Total applications)\s+(\d+)',
            r'(RAPIDS candidates)\s+(\d+)',
            r'(Overall estimated speedup)\s+([1-9]+\.[0-9]+)',  # Estimated speedup cannot be LT 1.00
            r'(Overall estimated cost savings)\s+([+-]?[0-9]+\.[0-9]+)%',
            r'Worker nodes have no local SSDs\. Local SSD is recommended for Spark scratch space to improve IO',
            r'Cost estimation is based on 1 local SSD per worker',
            r'To launch a GPU-accelerated cluster with RAPIDS Accelerator for Apache Spark',
            r'--initialization-actions=gs://',
            r'--cuda-version=([1-9]+\.[0-9]+)'
        ]
    }
    with patch.object(CMDRunner, 'gcloud_describe_cluster',
                      side_effect=[mock_cluster_props(f'{cluster_name}')]) as pull_cluster_props, \
            patch.object(CMDRunner, 'gcloud_cp',
                         side_effect=events_scenario.get('mockFunc')) as mock_remote_copy, \
            patch.object(Qualification, '_run_tool_as_spark',
                         side_effect=mock_successful_run_as_spark) as mock_submit_as_spark:
        with ArgvContext('spark_rapids_dataproc',
                         'qualification',
                         '--cluster', f'{cluster_name}',
                         '--region', 'us-central1',
                         '--output_folder', f'{qual_output_dir}'):
            dataproc_wrapper.main()
    # check the output on local disk
    if scenario_id == 'no_events':
        assert not os.path.exists(f'{wrapper_output_folder}')
    else:
        assert os.path.exists(f'{wrapper_summary_file}'), 'Summary report was not generated!'
    # check the stdout and log output
    captured_output = capsys.readouterr()
    for stdout_reg in std_reg_expressions.get(scenario_id):
        assert re.search(stdout_reg, captured_output.out), \
            f'Could not match {stdout_reg} in tool output in {scenario_id} scenario'
    for log_reg in log_reg_expressions:
        assert re.search(log_reg, captured_output.err), \
            f'Could not match {log_reg} in tool log in {scenario_id} scenario'

    pull_cluster_props.assert_called_once()
    mock_submit_as_spark.assert_called_once()
    assert mock_remote_copy.call_count == 2
