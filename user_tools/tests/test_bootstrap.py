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
import yaml
from cli_test_helpers import ArgvContext  # pylint: disable=import-error

from spark_rapids_dataproc_tools import dataproc_wrapper
from spark_rapids_dataproc_tools.dataproc_utils import DataprocClusterPropContainer
from spark_rapids_dataproc_tools.rapids_models import Bootstrap


def simulate_gcloud_pull_gpu_mem_err(scenario: str):
    msg_lookup = {
        'non-running': (
            'Failure Running Rapids Tool (Bootstrap).'
            '\n\tCould not ssh to cluster or Cluster does not support GPU. '
            'Make sure the cluster is running and NVIDIA drivers are installed.'
            '\n\tRun Terminated with error.'
            'Error invoking CMD <gcloud compute ssh dataproc-test-gpu-cluster '
            "--zone=us-central1-a --command='nvidia-smi --query-gpu=memory.total --format=csv,noheader'>: "
            '\n\t| ERROR: (gcloud.compute.ssh) [/usr/bin/ssh] exited with return code [255].'),
        'nvidia-smi-not-found': (
            'Failure Running Rapids Tool (Bootstrap).'
            '\n\tCould not ssh to cluster or Cluster does not support GPU. '
            'Make sure the cluster is running and NVIDIA drivers are installed.'
            '\n\tRun Terminated with error.'
            'Error invoking CMD <gcloud compute ssh dataproc-test-gpu-cluster '
            "--zone=us-central1-a --command='nvidia-smi --query-gpu=memory.total --format=csv,noheader'>: "
            '\n\t| bash: nvidia-smi: command not found')
    }
    err_msg = msg_lookup.get(scenario)
    raise RuntimeError(f'{err_msg}')


def mock_non_running_ssh():
    simulate_gcloud_pull_gpu_mem_err('non-running')


def mock_no_gpu_driver():
    simulate_gcloud_pull_gpu_mem_err('nvidia-smi-not-found')


def mock_cluster_props(cluster: str, **unused_kwargs):
    with open(f'tests/resources/dataproc-{cluster}.yaml', 'r', encoding='utf-8') as yaml_file:
        static_properties = yaml.safe_load(yaml_file)
        return yaml.dump(static_properties)


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
    test_output_folder = tmp_path / 'bootstrap'
    return test_output_folder


@patch.object(DataprocClusterPropContainer, '_pull_worker_gpu_memories', mock_non_running_ssh)
@patch.object(Bootstrap, '_pull_cluster_properties', side_effect=[mock_cluster_props('test-nongpu-cluster')])
def test_bootstrap_failure_non_running_cluster(capsys, boot_output_dir):
    """Test Bootstrap with non-running cluster."""
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
    # | gcloud compute ssh ahussein-dp-spark3-gpu-w-0 --project=rapids-spark --zone=us-central1-a --troubleshoot
    # |
    # | Or, to investigate an IAP tunneling issue:
    # |
    # | gcloud compute ssh ahussein-dp-spark3-gpu-w-0 --project=rapids-spark --zone=us-central1-a \
    # --troubleshoot --tunnel-through-iap
    # |
    # | ERROR: (gcloud.compute.ssh) [/usr/bin/ssh] exited with return code [255].
    # Run the actual test
    with pytest.raises(SystemExit):
        with ArgvContext('spark_rapids_dataproc', 'bootstrap',
                         '--cluster', 'dataproc-test-gpu-cluster',
                         '--region', 'us-central1',
                         '--output_folder', f'{boot_output_dir}'):
            dataproc_wrapper.main()
    captured_output = capsys.readouterr()
    main_key_stmts = [
        'Failure Running Rapids Tool (Bootstrap).',
        'Could not ssh to cluster',
        'ERROR: (gcloud.compute.ssh)'
    ]
    assert all(captured_output.out.strip().find(stmt) != -1 for stmt in main_key_stmts)
    assert not os.path.exists(
        f'{boot_output_dir}/wrapper-output/rapids_user_tools_bootstrap/bootstrap_tool_output'
    )


@patch.object(DataprocClusterPropContainer, '_pull_worker_gpu_memories', mock_no_gpu_driver)
@patch.object(Bootstrap, '_pull_cluster_properties', side_effect=[mock_cluster_props('test-nongpu-cluster')])
def test_bootstrap_failure_on_non_gpu_cluster(capsys, boot_output_dir):
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
    def check_failure_content(actual_captured: str) -> bool:
        main_key_stmts = [
            'Failure Running Rapids Tool (Bootstrap).',
            'Could not ssh to cluster or Cluster does not support GPU.',
            'bash: nvidia-smi: command not found'
        ]
        return all(actual_captured.find(stmt) != -1 for stmt in main_key_stmts)

    with pytest.raises(SystemExit):
        with ArgvContext('spark_rapids_dataproc', 'bootstrap',
                         '--cluster', 'dataproc-test-nongpu-cluster',
                         '--region', 'us-central1',
                         '--output_folder', f'{boot_output_dir}'):
            dataproc_wrapper.main()
    captured_output = capsys.readouterr().out
    check_failure_content(captured_output)
    assert not os.path.exists(
        f'{boot_output_dir}/wrapper-output/rapids_user_tools_bootstrap/bootstrap_tool_output'
    )


def test_bootstrap_failure_non_existing_cluster(capfd, boot_output_dir):
    """Test Bootstrap with non-existing cluster."""
    # Expected output running on invalid cluster
    # Failure Running Rapids Tool (Bootstrap).
    # Could not pull Cluster description region:us-central1, dataproc-test-cluster
    # Run Terminated with error.
    #     Error invoking CMD <gcloud dataproc clusters describe dataproc-test-cluster --region=us-central1>:
    # | ERROR: (gcloud.dataproc.clusters.describe) NOT_FOUND: Not found: \
    # Cluster projects/rapids-spark/regions/us-central1/clusters/dataproc-test-cluster

    def check_failure_content(actual_captured: str) -> bool:
        main_key_stmts = [
            'Failure Running Rapids Tool (Bootstrap).',
            'Could not pull Cluster description',
            'ERROR: (gcloud.dataproc.clusters.describe) NOT_FOUND: Not found:'
        ]
        return all(actual_captured.find(stmt) != -1 for stmt in main_key_stmts)

    # Run the actual test
    boot_args = [
        '--region=us-central1',
        '--cluster=dataproc-test-non-existing-cluster',
        f'--output_folder={boot_output_dir}'
    ]
    wrapper_args = ' '.join(boot_args)
    run_dataproc_cmd(capfd,
                     cmd=f'bootstrap {wrapper_args}',
                     expected=1,
                     func_cb=check_failure_content)
    assert not os.path.exists(
        f'{boot_output_dir}/wrapper-output/rapids_user_tools_bootstrap/bootstrap_tool_output/'
    )


def test_bootstrap_failure_applying_changes_on_driver(capsys, boot_output_dir):
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
                      side_effect=[mock_cluster_props('test-gpu-cluster')]) as pull_cluster_props, \
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
        f'{boot_output_dir}/wrapper-output/rapids_user_tools_bootstrap/bootstrap_tool_output'
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
                      side_effect=[mock_cluster_props('test-gpu-cluster')]) as pull_cluster_props, \
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
        f'{boot_output_dir}/wrapper-output/rapids_user_tools_bootstrap/bootstrap_tool_output/'
        'rapids_4_dataproc_bootstrap_output.log'
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
                      side_effect=[mock_cluster_props('test-gpu-cluster')]) as pull_cluster_props, \
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
    assert apply_remote_configs.call_count == 0
    assert os.path.exists(
        f'{boot_output_dir}/wrapper-output/rapids_user_tools_bootstrap/bootstrap_tool_output/'
        'rapids_4_dataproc_bootstrap_output.log'
    )
