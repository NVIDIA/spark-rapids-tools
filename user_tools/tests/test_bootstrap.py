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

import os
import subprocess
from unittest.mock import patch

import pytest  # pylint: disable=import-error
from cli_test_helpers import ArgvContext  # pylint: disable=import-error

from conftest import mock_cluster_props, get_wrapper_work_dir, mock_pull_gpu_mem_no_gpu_driver, \
    RapidsToolTestBasic, dir_exists, mock_success_pull_cluster_props
from spark_rapids_dataproc_tools import dataproc_wrapper
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
    return cluster_gpu_info.get(f'dataproc-{cluster}')


def mock_gcloud_remote_configs(*unused_args):
    """
    Mock that guarantees that we do not fail when we run ssh command to apply bootstrap changes
    to remote driver node
    """


def run_dataproc_cmd(capfd, cmd: str, expected: int = 0, func_cb=None):
    # pylint: disable=subprocess-run-check
    c = subprocess.run(f'spark_rapids_dataproc {cmd}', shell=True, text=True)
    captured_output = capfd.readouterr()
    with capfd.disabled():
        if func_cb is not None:
            # print(captured_output)
            func_cb(captured_output.out.strip())
    assert expected == c.returncode


@pytest.fixture(name='boot_output_dir')
def fixture_boot_output_dir(tmp_path):
    """
    Placeholder to prepare the output directory passed to the Bootstrap command.
    Note that there is no need to create the parent directory because the wrapper internally does that.
    """
    test_output_folder = tmp_path / 'boot'
    return test_output_folder


def boot_result_dir(root_out_directory):
    return get_wrapper_work_dir('bootstrap', root_out_directory)


class TestBootstrap(RapidsToolTestBasic):
    """Test Bootstrap features."""
    def _init_tool_ctx(self, autofilled_ctx: dict):
        tool_name = 'bootstrap'
        prof_ctxt = {
            'tool_name': tool_name,
            'wrapper_out_dirname': 'bootstrap-tool-output',
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

    @patch.object(DataprocClusterPropContainer, '_pull_worker_gpu_memories', mock_pull_gpu_mem_no_gpu_driver)
    @patch.object(CMDRunner, 'gcloud_describe_cluster', side_effect=mock_success_pull_cluster_props)
    def test_bootstrap_fail_on_non_gpu_cluster(self, ut_dir, submission_cluster='dataproc-test-nongpu-cluster'):
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
        self.run_fail_wrapper(submission_cluster, ut_dir)
        self.assert_output_as_expected(std_reg_expressions)
        assert not dir_exists(self.get_wrapper_out_dir(ut_dir)), \
            f'Directory {self.get_wrapper_out_dir(ut_dir)} exists!!!'


def test_bootstrap_fail_applying_changes_on_driver(capsys, boot_output_dir):
    """Test Bootstrap failure running command to apply changes on driver node."""
    # Failure Running Rapids Tool (Bootstrap).
    #         Could not apply configurations changes to remote cluster dataproc-test-gpu-cluster
    #         Run Terminated with error.
    #         Error while running gcloud ssh command on remote cluster ;\
    #         ERROR: (gcloud.compute.ssh) Could not fetch resource:
    #  - The resource \w was not found
    main_key_stmts = [
        'Failure Running Rapids Tool (Bootstrap).',
        'Could not apply configurations changes to remote cluster dataproc-test-gpu-cluster',
        '(gcloud.compute.ssh) Could not fetch resource'
    ]
    with patch.object(Bootstrap, '_pull_cluster_properties',
                      side_effect=[mock_cluster_props('dataproc-test-gpu-cluster')]) as pull_cluster_props, \
            patch.object(DataprocClusterPropContainer, '_pull_worker_gpu_memories',
                         side_effect=[mock_pull_gpu_memories('test-gpu-cluster')]) as pull_gpu_mem:
        with pytest.raises(SystemExit):
            with ArgvContext('spark_rapids_dataproc',
                             'bootstrap', '--cluster',
                             'dataproc-test-gpu-cluster',
                             '--region', 'us-central1',
                             '--debug', 'True',
                             '--output_folder', f'{boot_output_dir}'):
                dataproc_wrapper.main()
    captured_output = capsys.readouterr().out
    assert all(captured_output.find(stmt) != -1 for stmt in main_key_stmts)
    pull_cluster_props.assert_called_once()
    pull_gpu_mem.assert_called_once()
    assert not os.path.exists(
        f'{boot_result_dir(boot_output_dir)}/bootstrap_tool_output'
    )


def test_bootstrap_success_with_t4(capsys, caplog, boot_output_dir):
    """
    Verify that the calculations of the bootstrap given the cluster properties defined in
    tests/resources/dataproc-test-gpu-cluster.yaml, which runs T4 Nvidia GPUs.
    """
    main_key_stmts = [
        'Recommended configurations are saved to local disk:',
        'wrapper-output/rapids_user_tools_bootstrap/bootstrap_tool_output/rapids_4_dataproc_bootstrap_output.log',
        'Using the following computed settings based on worker nodes:',
        '##### BEGIN : RAPIDS bootstrap settings for dataproc-test-gpu-cluster',
        'spark.executor.cores=16',
        'spark.executor.memory=32768m',
        'spark.executor.memoryOverhead=7372m',
        'spark.rapids.sql.concurrentGpuTasks=2',
        'spark.rapids.memory.pinnedPool.size=4096m',
        'spark.sql.files.maxPartitionBytes=512m',
        'spark.task.resource.gpu.amount=0.0625',
        '##### END : RAPIDS bootstrap settings for dataproc-test-gpu-cluster']

    with patch.object(Bootstrap, '_pull_cluster_properties',
                      side_effect=[mock_cluster_props('dataproc-test-gpu-cluster')]) as pull_cluster_props, \
            patch.object(DataprocClusterPropContainer, '_pull_worker_gpu_memories',
                         side_effect=[mock_pull_gpu_memories('test-gpu-cluster')]) as pull_gpu_mem, \
            patch.object(Bootstrap, '_apply_changes_to_remote_cluster',
                         side_effect=[mock_gcloud_remote_configs]) as apply_remote_configs:
        with ArgvContext('spark_rapids_dataproc',
                         'bootstrap', '--cluster',
                         'dataproc-test-gpu-cluster',
                         '--region', 'us-central1',
                         '--output_folder', f'{boot_output_dir}'):
            dataproc_wrapper.main()

    captured_run = capsys.readouterr()
    captured_out = captured_run.out
    assert all(captured_out.find(stmt) != -1 for stmt in main_key_stmts)
    assert caplog.text.find('Applying the configuration to remote cluster ') != 1
    # assert func.call_count == 1
    pull_cluster_props.assert_called_once()
    pull_gpu_mem.assert_called_once()
    apply_remote_configs.assert_called_once()
    assert os.path.exists(
        f'{boot_result_dir(boot_output_dir)}/bootstrap_tool_output/rapids_4_dataproc_bootstrap_output.log'
    )


def test_bootstrap_success_with_t4_dry_run(capsys, caplog, boot_output_dir):
    """
    Passing the dry_run mode, verify that the changes won't be applied to the cluster.
    This is achieved by checking the content of log messages.
    Verify that the calculations of the bootstrap given the cluster properties defined in
    tests/resources/dataproc-test-gpu-cluster.yaml, which runs T4 Nvidia GPUs.
    """
    # When the dry_run is enabled the loginfo should contain the following message:
    # 'Skipping applying configurations to remote cluster {self.cluster}.  DRY_RUN is enabled.'
    main_key_stmts = [
        'Recommended configurations are saved to local disk:',
        'wrapper-output/rapids_user_tools_bootstrap/bootstrap_tool_output/rapids_4_dataproc_bootstrap_output.log',
        'Using the following computed settings based on worker nodes:',
        '##### BEGIN : RAPIDS bootstrap settings for dataproc-test-gpu-cluster',
        'spark.executor.cores=16',
        'spark.executor.memory=32768m',
        'spark.executor.memoryOverhead=7372m',
        'spark.rapids.sql.concurrentGpuTasks=2',
        'spark.rapids.memory.pinnedPool.size=4096m',
        'spark.sql.files.maxPartitionBytes=512m',
        'spark.task.resource.gpu.amount=0.0625',
        '##### END : RAPIDS bootstrap settings for dataproc-test-gpu-cluster']

    with patch.object(Bootstrap, '_pull_cluster_properties',
                      side_effect=[mock_cluster_props('dataproc-test-gpu-cluster')]) as pull_cluster_props, \
            patch.object(DataprocClusterPropContainer, '_pull_worker_gpu_memories',
                         side_effect=[mock_pull_gpu_memories('test-gpu-cluster')]) as pull_gpu_mem, \
            patch.object(Bootstrap, '_apply_changes_to_remote_cluster',
                         side_effect=[mock_gcloud_remote_configs]) as apply_remote_configs:
        with ArgvContext('spark_rapids_dataproc',
                         'bootstrap', '--cluster',
                         'dataproc-test-gpu-cluster',
                         '--region', 'us-central1',
                         '--dry_run', 'True',
                         '--output_folder', f'{boot_output_dir}'):
            dataproc_wrapper.main()
    captured_run = capsys.readouterr()
    captured_out = captured_run.out
    assert all(captured_out.find(stmt) != -1 for stmt in main_key_stmts)
    assert caplog.text.find('Skipping applying configurations to remote cluster') != 1
    # assert func.call_count == 1
    pull_cluster_props.assert_called_once()
    pull_gpu_mem.assert_called_once()
    apply_remote_configs.assert_not_called()
    assert os.path.exists(
        f'{boot_result_dir(boot_output_dir)}/bootstrap_tool_output/rapids_4_dataproc_bootstrap_output.log'
    )
