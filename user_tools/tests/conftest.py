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

"""Add common helpers and utilities"""

import os
import re
from typing import Optional
from unittest.mock import patch, call

import pytest  # pylint: disable=import-error
import yaml
from cli_test_helpers import ArgvContext  # pylint: disable=import-error

from spark_rapids_dataproc_tools import dataproc_wrapper
from spark_rapids_dataproc_tools.dataproc_utils import CMDRunner


def mock_success_pull_cluster_props(*argvs, **unused_kwargs):
    if len(argvs) >= 2:
        # first argument is cluster_name
        # second argument is region_name
        with open(f'tests/resources/{argvs[0]}.yaml', 'r', encoding='utf-8') as yaml_file:
            static_properties = yaml.safe_load(yaml_file)
            return yaml.dump(static_properties)
    return ''


def mock_pull_gpu_mem_non_running_cluster():
    simulate_gcloud_pull_gpu_mem_err('non-running')


def mock_pull_gpu_mem_no_gpu_driver(*unused_argv):
    simulate_gcloud_pull_gpu_mem_err('nvidia-smi-not-found')


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


def assert_patterns_in_output(reg_expressions, captured_out, msg: Optional[str] = None):
    for stdout_reg in reg_expressions:
        assert re.search(stdout_reg, captured_out), f'Could not match {stdout_reg} in output. {msg}'


def get_wrapper_work_dir(tool_name, root_dir):
    """
    Given the tmp path of the unit tests, get the working directory of the wrapper.
    :param tool_name: the name of tool (qualification/profiling/bootstrap)
    :param root_dir: the tmp path of the unit test
    :return: the working directory of the wrapper
    """
    # TODO: load constants from the configuration files src/resources/*.yaml.
    return f'{root_dir}/wrapper-output/rapids_user_tools_{tool_name}'


def os_path_exists(filesys_path):
    return os.path.exists(filesys_path)


class RapidsToolTestBasic:
    """Basic class that encapsulates common methods and setup."""
    tool_ctxt = {}

    def _init_tool_ctx(self, autofilled_ctx: dict):
        self.tool_ctxt.update(autofilled_ctx)

    def get_tool_name(self):
        return self.tool_ctxt['tool_name']

    def get_work_dir_postfix(self):
        return self.tool_ctxt['work_dir_postfix']

    def get_capsys(self):
        return self.tool_ctxt['capsys']

    def get_work_dir(self, ut_dir):
        return f'{ut_dir}/{self.get_work_dir_postfix()}'

    def get_wrapper_out_dir(self, ut_dir):
        work_dir = f'{self.get_work_dir(ut_dir)}'
        out_dir_name = self.tool_ctxt['wrapper_out_dirname']
        tool_out_dir = os.path.join(work_dir, out_dir_name)
        return tool_out_dir

    @pytest.fixture(autouse=True)
    def init_tool(self, capsys):
        """Capsys hook into this class"""
        self.tool_ctxt['capsys'] = capsys
        self._init_tool_ctx(autofilled_ctx={})

    @pytest.fixture(name='ut_dir')
    def ut_dir(self, tmp_path):
        return tmp_path

    def print_to_console(self, msg):
        """Print strOut to console (even within a pyTest execution)"""
        with self.get_capsys().disabled():
            print(msg)

    def get_default_args(self, cluster_name, ut_dir):
        return [
            'spark_rapids_dataproc',
            f'{self.get_tool_name()}',
            '--cluster', f'{cluster_name}',
            '--region', 'us-central1',
            '--output_folder', f'{ut_dir}']

    def capture_wrapper_output(self) -> (str, str):
        captured_out = self.get_capsys().readouterr()
        return captured_out.out, captured_out.err

    def run_successful_wrapper(self, cluster_name, ut_dir, extra_wrapper_args=None):
        wrapper_args = self.get_default_args(cluster_name, ut_dir)
        if extra_wrapper_args is not None:
            wrapper_args.extend(extra_wrapper_args)
        with ArgvContext(*wrapper_args):
            dataproc_wrapper.main()

    def run_fail_wrapper(self, cluster_name, ut_dir, extra_wrapper_args=None):
        wrapper_args = self.get_default_args(cluster_name, ut_dir)
        if extra_wrapper_args is not None:
            wrapper_args.extend(extra_wrapper_args)
        with pytest.raises(SystemExit):
            with ArgvContext(*wrapper_args):
                dataproc_wrapper.main()

    def assert_work_dir_exists(self, ut_root_dir):
        work_dir = self.get_work_dir(ut_root_dir)
        assert os_path_exists(work_dir), f'Working directory {work_dir} exists!!'

    def assert_work_dir_not_exists(self, ut_root_dir):
        work_dir = self.get_work_dir(ut_root_dir)
        assert not os_path_exists(work_dir), f'Working directory {work_dir} exists!!'

    def assert_wrapper_out_dir_not_exists(self, ut_root_dir):
        out_dir = self.get_wrapper_out_dir(ut_root_dir)
        assert not os_path_exists(out_dir), f'Wrapper output directory {out_dir} exists!!'

    def assert_wrapper_out_dir_exists(self, ut_root_dir):
        out_dir = self.get_wrapper_out_dir(ut_root_dir)
        assert os_path_exists(out_dir), f'Wrapper output directory {out_dir} does not exist!!'

    def assert_output_as_expected(self,
                                  std_regs,
                                  log_regs=None,
                                  std_msg: Optional[str] = 'Stdout has no match.',
                                  log_msg: Optional[str] = 'Log-output has no match.'):
        cap_out, cap_err = self.capture_wrapper_output()
        if std_regs is not None:
            assert_patterns_in_output(std_regs, cap_out, std_msg)
        if log_regs is not None:
            assert_patterns_in_output(log_regs, cap_err, log_msg)

    def _run_tool_on_spark2(self, tool_name,
                            ut_root_dir,
                            submission_cluster='dataproc-test-spark2-cluster'):
        """Qualification/Profiling Tools cannot run on Spark2.x clusters."""
        # 2022-12-07 11:14:12,582 WARNING qualification: The cluster image 1.5.75-debian10 is not supported. \
        # To support the RAPIDS user tools, you will need to use an image that runs Spark3.x.
        # Failure Running Rapids Tool (Profiling).
        #         Tool cannot execute on the execution cluster.
        #         Run Terminated with error.
        #         The cluster image 1.5.75-debian10 is not supported. To support the RAPIDS user tools, \
        #         you will need to use an image that runs Spark3.x.
        std_reg_expressions = [
            rf'Failure Running Rapids Tool \({tool_name.capitalize()}\)\.',
            r'Tool cannot execute on the execution cluster\.'
        ]
        log_reg_expressions = [
            rf'WARNING {tool_name}: The cluster image 1\.5\.75-debian10 is not supported',
            r'To support the RAPIDS user tools, you will need to use an image that runs Spark3\.x\.'
        ]
        with patch.object(CMDRunner, 'gcloud_describe_cluster',
                          side_effect=mock_success_pull_cluster_props) as pull_cluster_props:
            self.run_fail_wrapper(submission_cluster, ut_root_dir)
        self.assert_output_as_expected(std_reg_expressions, log_reg_expressions)
        pull_cluster_props.assert_called_once()
        expected_pull_cluster_args = [
            call(f'{submission_cluster}',
                 'us-central1',
                 f'Could not pull Cluster description region:us-central1, {submission_cluster}')]
        assert pull_cluster_props.call_args_list == expected_pull_cluster_args
        self.assert_work_dir_not_exists(ut_root_dir)

    def _run_tool_on_non_running_gpu_cluster(self,
                                             ut_root_dir,
                                             std_reg_expressions,
                                             log_reg_expressions=None,
                                             submission_cluster='dataproc-test-gpu-cluster'):
        expected_pull_cluster_args = [
            call(f'{submission_cluster}',
                 'us-central1',
                 f'Could not pull Cluster description region:us-central1, {submission_cluster}')]
        with patch.object(CMDRunner, 'gcloud_describe_cluster',
                          side_effect=mock_success_pull_cluster_props) as pull_cluster_props:
            self.run_fail_wrapper(submission_cluster, ut_root_dir)
        self.assert_output_as_expected(std_reg_expressions, log_regs=log_reg_expressions)
        self.assert_work_dir_not_exists(ut_root_dir)
        assert pull_cluster_props.call_args_list == expected_pull_cluster_args
