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

import tarfile
from unittest.mock import patch

import pytest  # pylint: disable=import-error

from conftest import mock_cluster_props, get_wrapper_work_dir, RapidsToolTestBasic, dir_exists
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


class TestQualification(RapidsToolTestBasic):
    """Test Qualification features."""
    def _init_tool_ctx(self, autofilled_ctx: dict):
        tool_name = 'qualification'
        prof_ctxt = {
            'tool_name': tool_name,
            'work_dir_postfix': f'wrapper-output/rapids_user_tools_{tool_name}',
            'wrapper_out_dirname': 'qual-tool-output'
        }
        autofilled_ctx.update(prof_ctxt)
        super()._init_tool_ctx(autofilled_ctx)

    def test_fail_on_spark2(self, ut_dir):
        """qualification tool does not run on spark2.x"""
        self._run_tool_on_spark2(self.get_tool_name(), ut_dir)

    @pytest.mark.parametrize('submission_cluster', ['dataproc-test-gpu-cluster', 'dataproc-test-nongpu-cluster'])
    def test_fail_non_running_cluster(self, ut_dir, submission_cluster):
        """
        Qualification runs on non GPU cluster as well as GPU cluster.
        This unit test verifies the error due to run qualification tool for an existing cluster; but it is not
        started at the time of submission.
        Note that we need to pass the first command of gcloud_cp that copies the dependencies from local
        disk to the remote storage.
        """
        # pylint: disable=line-too-long

        # spark_rapids_dataproc qualification --region=us-central1 --cluster=CLUSTER_NAME \
        # --output_folder={qual_output_dir}
        # 2022-12-07 12:43:12,442 INFO qualification: The original CPU cluster is the same as the \
        # submission cluster on which the tool runs. To update the configuration of the CPU cluster, \
        # make sure to pass the properties file to the CLI arguments.
        # 2022-12-07 12:43:12,443 INFO qualification: The GPU cluster is the same as the submission \
        # cluster on which the RAPIDS tool is running [ahussein-dp-spark3]. To update the configuration \
        # of the GPU cluster, make sure to pass the properties file to the CLI arguments.
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
            (r'INFO qualification: The original CPU cluster is the same as the submission cluster on '
             r'which the tool runs\. To update the configuration of the CPU cluster, make sure to pass '
             r'the properties file to the CLI arguments'),
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
        # Note that we need to pass the phase in which the wrapper upload dependencies to the remote storage.
        # This is mainly because storage can still be accessible even when the cluster is offline.
        with patch.object(CMDRunner, 'gcloud_cp', side_effect=mock_passive_copy):
            self._run_tool_on_non_running_gpu_cluster(ut_dir,
                                                      std_reg_expressions,
                                                      log_reg_expressions,
                                                      submission_cluster=submission_cluster)

    @pytest.mark.parametrize('events_scenario',
                             [{'label': 'with_events', 'mockFunc': mock_remote_download},
                              {'label': 'no_events', 'mockFunc': mock_passive_copy}],
                             ids=('with tool output', 'No tool output'))
    @pytest.mark.parametrize('submission_cluster',
                             ['dataproc-test-gpu-cluster', 'dataproc-test-nongpu-cluster'])
    def test_qual_success(self, ut_dir, submission_cluster, events_scenario):
        """
        This is a successful run achieved by capturing cloud_copy command. It is executed with two different scenarios:
        1- with empty eventlogs, and 2- with mocked eventlogs loaded from resources folder.
        For more details on the sample see resources/output_samples/generation.md
        :param capsys: captures the stdout of the execution.
        :param qual_output_dir: the temp folder passed as an argument to the wrapper command.
        :param submission_cluster: the name of the cluster used to submit the tool.
        :param events_scenario: the method to be used as side effect for cloud copy command.
        """

        # pylint: disable=line-too-long

        # If the Qualification tool generates no output, it is expected to get the following output
        # Configuration Incompatibilities:
        # --------------- --------------------------------------------------------------------------------------------------
        # workerLocalSSDs - Worker nodes have no local SSDs. Local SSD is recommended for Spark scratch space to improve IO.
        # --------------- --------------------------------------------------------------------------------------------------
        # The Qualification tool did not generate any output. Nothing to display.

        # pylint: enable=line-too-long

        scenario_id = events_scenario.get('label')
        wrapper_output_folder = self.get_wrapper_out_dir(ut_dir)
        wrapper_summary_file = f'{wrapper_output_folder}/rapids_4_dataproc_qualification_output.csv'
        rapids_qual_folder = f'{wrapper_output_folder}/rapids_4_spark_qualification_output'
        log_reg_expressions = [
            (r'INFO qualification: The original CPU cluster is the same as the submission cluster on which the tool '
             r'runs\. To update the configuration of the CPU cluster, make sure to pass the properties file to '
             r'the CLI arguments'),
            (r'INFO qualification: The GPU cluster is the same as the submission cluster on which the RAPIDS tool is '
             rf'running \[{submission_cluster}\]\. To update the configuration of the GPU cluster, '
             r'make sure to pass the properties file to the CLI arguments\.'),
        ]
        std_reg_expressions = {
            'no_events': [
                r'Configuration Incompatibilities:',
                (r'workerLocalSSDs  '
                 r'- Worker nodes have no local SSDs\. Local SSD is recommended for Spark scratch '
                 r'space to improve IO\.'),
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
                          side_effect=[mock_cluster_props(f'{submission_cluster}')]) as pull_cluster_props, \
                patch.object(CMDRunner, 'gcloud_cp',
                             side_effect=events_scenario.get('mockFunc')) as mock_remote_copy, \
                patch.object(Qualification, '_run_tool_as_spark',
                             side_effect=mock_successful_run_as_spark) as mock_submit_as_spark:
            self.run_successful_wrapper(submission_cluster, ut_dir)
        # check the output on local disk
        if scenario_id == 'no_events':
            assert not dir_exists(f'{wrapper_output_folder}')
        else:
            assert dir_exists(f'{wrapper_summary_file}'), 'Summary report was not generated!'
        # check the stdout and log output
        self.assert_output_as_expected(std_reg_expressions.get(scenario_id),
                                       log_reg_expressions)
        pull_cluster_props.assert_called_once()
        mock_submit_as_spark.assert_called_once()
        assert mock_remote_copy.call_count == 2
