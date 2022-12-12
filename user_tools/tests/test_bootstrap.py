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
"""Test Bootstrap functions."""

from unittest.mock import patch

import pytest  # pylint: disable=import-error

from conftest import mock_pull_gpu_mem_no_gpu_driver, \
    RapidsToolTestBasic, os_path_exists, mock_success_pull_cluster_props
from spark_rapids_dataproc_tools.dataproc_utils import DataprocClusterPropContainer, CMDRunner
from spark_rapids_dataproc_tools.rapids_models import Bootstrap


def mock_pull_gpu_memories(cluster: str):
    """
    A mock implementation to simulate the result of running SSH command on the dataproc cluster
    to retrieve the gpu info
    compute ssh <worker-node> --zone=<zone> --command='nvidia-smi --query-gpu=memory.total --format=csv,noheader'
    For the given sample cluster in tests/resources/dataproc-test-gpu-cluster.yaml, the output is something like:
    15109 MiB
    15109 MiB
    """
    cluster_gpu_info = {
        'dataproc-test-gpu-cluster': '\n'.join(['15109 MiB', '15109 MiB'])
    }
    return cluster_gpu_info.get(cluster)


def mock_gcloud_remote_configs(*unused_args):
    """
    Mock that guarantees that we do not fail when we run ssh command to apply bootstrap changes
    to remote driver node
    """


class TestBootstrap(RapidsToolTestBasic):
    """Test Bootstrap features."""
    def _init_tool_ctx(self, autofilled_ctx: dict):
        tool_name = 'bootstrap'
        prof_ctxt = {
            'tool_name': tool_name,
            'wrapper_out_dirname': 'bootstrap_tool_output',
            'work_dir_postfix': f'wrapper-output/rapids_user_tools_{tool_name}'
        }
        autofilled_ctx.update(prof_ctxt)
        super()._init_tool_ctx(autofilled_ctx)

    @pytest.mark.parametrize('submission_cluster', ['dataproc-test-gpu-cluster', 'dataproc-test-nongpu-cluster'])
    def test_fail_non_running_cluster(self, ut_dir, submission_cluster):
        # Failure Running Rapids Tool (Bootstrap).
        # Could not ssh to cluster or Cluster does not support GPU. Make sure the cluster is running \
        # and Please install NVIDIA drivers.
        # Run Terminated with error.
        #     Error invoking CMD <gcloud compute ssh dataproc-test-gpu-cluster-w-0 --zone=us-central1-a \
        #     --command='nvidia-smi \
        #     --query-gpu=memory.total --format=csv,noheader'>:
        # | External IP address was not found; defaulting to using IAP tunneling.
        # | ERROR: (gcloud.compute.start-iap-tunnel) Error while connecting [4033: 'not authorized'].
        #     | kex_exchange_identification: Connection closed by remote host
        # | Connection closed by UNKNOWN port 65535
        # |
        # | Recommendation: To check for possible causes of SSH connectivity issues and get
        # | recommendations, rerun the ssh command with the --troubleshoot option.
        # |
        # | gcloud compute ssh dataproc-test-gpu --project=rapids-spark --zone=us-central1-a --troubleshoot
        # |
        # | Or, to investigate an IAP tunneling issue:
        # |
        # | gcloud compute ssh dataproc-test-gpu-w-0 --project=rapids-spark --zone=us-central1-a \
        # --troubleshoot --tunnel-through-iap
        # |
        # | ERROR: (gcloud.compute.ssh) [/usr/bin/ssh] exited with return code [255].
        std_reg_expressions = [
            rf'Failure Running Rapids Tool \({self.get_tool_name().capitalize()}\)\.',
            r'Could not ssh to cluster',
            r'ERROR: \(gcloud\.compute\.ssh\)'
        ]
        self._run_tool_on_non_running_gpu_cluster(ut_dir, std_reg_expressions, submission_cluster=submission_cluster)

    def test_fail_on_non_gpu_cluster(self, ut_dir, submission_cluster='dataproc-test-nongpu-cluster'):
        """
        Running bootstrap on non gpu cluster should fail.
        """
        # Failure Running Rapids Tool (Bootstrap).
        #         Could not ssh to cluster or Cluster does not support GPU. Make sure the cluster is \
        #         running and NVIDIA drivers are installed.
        #         Run Terminated with error.
        #         Error invoking CMD <gcloud compute ssh node-w-0 --zone=us-central1-a \
        #         --command='nvidia-smi --query-gpu=memory.total --format=csv,noheader'>:
        #         | bash: nvidia-smi: command not found
        std_reg_expressions = [
            rf'Failure Running Rapids Tool \({self.get_tool_name().capitalize()}\)\.',
            r'Could not ssh to cluster or Cluster does not support GPU.',
            r'bash: nvidia-smi: command not found'
        ]
        with patch.object(DataprocClusterPropContainer, '_pull_worker_gpu_memories', mock_pull_gpu_mem_no_gpu_driver), \
                patch.object(CMDRunner, 'gcloud_describe_cluster', side_effect=mock_success_pull_cluster_props):
            self.run_fail_wrapper(submission_cluster, ut_dir)
        self.assert_output_as_expected(std_reg_expressions)
        assert not os_path_exists(self.get_wrapper_out_dir(ut_dir)), \
            f'Directory {self.get_wrapper_out_dir(ut_dir)} exists!!!'

    def test_fail_apply_changes_on_driver(self,
                                          ut_dir,
                                          submission_cluster='dataproc-test-gpu-cluster'):
        """Test Bootstrap failure running command to apply changes on driver node."""
        # Failure Running Rapids Tool (Bootstrap).
        #         Could not apply configurations changes to remote cluster dataproc-test-gpu-cluster
        #         Run Terminated with error.
        #         Error while running gcloud ssh command on remote cluster ;\
        #         ERROR: (gcloud.compute.ssh) Could not fetch resource:
        #  - The resource \w was not found
        std_reg_expressions = [
            rf'Failure Running Rapids Tool \({self.get_tool_name().capitalize()}\)\.',
            rf'Could not apply configurations changes to remote cluster {submission_cluster}',
            r'\(gcloud\.compute\.ssh\) Could not fetch resource'
        ]
        log_reg_expressions = [
            rf'Applying the configuration to remote cluster {submission_cluster}'
        ]
        with patch.object(CMDRunner, 'gcloud_describe_cluster',
                          side_effect=mock_success_pull_cluster_props) as pull_cluster_props, \
                patch.object(DataprocClusterPropContainer, '_pull_worker_gpu_memories',
                             side_effect=[mock_pull_gpu_memories('dataproc-test-gpu-cluster')]) as pull_gpu_mem:
            self.run_fail_wrapper(submission_cluster, ut_dir)
        self.assert_output_as_expected(std_reg_expressions, log_reg_expressions)
        pull_cluster_props.assert_called_once()
        pull_gpu_mem.assert_called_once()
        self.assert_wrapper_out_dir_not_exists(ut_dir)

    @pytest.mark.parametrize('dry_run_enabled', [False, True])
    def test_success_on_t4_with_dry_run(self,
                                        ut_dir,
                                        dry_run_enabled,
                                        submission_cluster='dataproc-test-gpu-cluster'):
        """
        Verify that the calculations of the bootstrap given the cluster properties defined in
        tests/resources/dataproc-test-gpu-cluster.yaml, which runs T4 Nvidia GPUs.
        Passing the dry_run mode, verify that the changes won't be applied to the cluster.
        This is achieved by checking the content of log messages.
        Verify that the calculations of the bootstrap given the cluster properties defined in
        tests/resources/dataproc-test-gpu-cluster.yaml, which runs T4 Nvidia GPUs.
        """
        std_reg_expressions = [
            r'Recommended configurations are saved to local disk:',
            r'wrapper-output/rapids_user_tools_bootstrap/bootstrap_tool_output/rapids_4_dataproc_bootstrap_output\.log',
            r'Using the following computed settings based on worker nodes:',
            r'##### BEGIN : RAPIDS bootstrap settings for dataproc-test-gpu-cluster',
            r'spark\.executor\.cores=\d+',
            r'spark\.executor\.memory=\d+m',
            r'spark\.executor\.memoryOverhead=\d+m',
            r'spark\.rapids\.sql\.concurrentGpuTasks=\d+',
            r'spark\.rapids\.memory\.pinnedPool\.size=\d+m',
            r'spark\.sql\.files\.maxPartitionBytes=\d+m',
            r'spark\.task\.resource\.gpu\.amount=([0-9]+\.[0-9]+)',
            r'##### END : RAPIDS bootstrap settings for dataproc-test-gpu-cluster'
        ]
        std_log_expressions = None
        if dry_run_enabled:
            std_log_expressions = [
                rf'Skipping applying configurations to remote cluster {submission_cluster}\. '
                r' DRY_RUN is enabled\.'
            ]
        extra_args = ['--dry_run', f'{dry_run_enabled}']
        with patch.object(CMDRunner, 'gcloud_describe_cluster',
                          side_effect=mock_success_pull_cluster_props) as pull_cluster_props, \
                patch.object(DataprocClusterPropContainer, '_pull_worker_gpu_memories',
                             side_effect=[mock_pull_gpu_memories('dataproc-test-gpu-cluster')]) as pull_gpu_mem, \
                patch.object(Bootstrap, '_apply_changes_to_remote_cluster',
                             side_effect=mock_gcloud_remote_configs) as apply_remote_configs:
            self.run_successful_wrapper(submission_cluster, ut_dir, extra_args)
        self.assert_output_as_expected(std_reg_expressions, std_log_expressions)
        summary_file = f'{self.get_wrapper_out_dir(ut_dir)}/rapids_4_dataproc_bootstrap_output.log'
        self.assert_wrapper_out_dir_exists(ut_dir)
        assert os_path_exists(summary_file), f'Summary file {summary_file} does not exist!!!'
        if dry_run_enabled:
            apply_remote_configs.assert_not_called()
        else:
            apply_remote_configs.assert_called_once()
        pull_cluster_props.assert_called_once()
        pull_gpu_mem.assert_called_once()
