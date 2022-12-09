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

"""Test Profiling functions."""

import pytest  # pylint: disable=import-error

from conftest import RapidsToolTestBasic


class TestProfiling(RapidsToolTestBasic):
    """Test Profiling features."""

    def _init_tool_ctx(self, autofilled_ctx: dict):
        tool_name = 'profiling'
        prof_ctxt = {
            'tool_name': tool_name,
            'work_dir_postfix': f'wrapper-output/rapids_user_tools_{tool_name}'
        }
        autofilled_ctx.update(prof_ctxt)
        super()._init_tool_ctx(autofilled_ctx)

    def test_fail_on_spark2(self, ut_dir):
        """
        Tools cannot run on Spark2.x clusters.
        The wrapper should fail and show meaningful output to the user.
        """
        self._run_tool_on_spark2(self.get_tool_name(), ut_dir)

    @pytest.mark.parametrize('submission_cluster', ['dataproc-test-gpu-cluster', 'dataproc-test-nongpu-cluster'])
    def test_fail_non_running_cluster(self, ut_dir, submission_cluster):
        """Test Profiling failure with non-running cluster."""

        # Failure Running Rapids Tool (Profiling).
        # Could not ssh to cluster or Cluster does not support GPU. Make sure the cluster is running and \
        # NVIDIA drivers are installed.
        # Run Terminated with error.
        #         Error invoking CMD <gcloud compute ssh dataproc-cluster-gpu-w-0 -\
        #         -zone=us-central1-a --command='nvidia-smi --query-gpu=memory.total --format=csv,noheader'>:
        #         | External IP address was not found; defaulting to using IAP tunneling.
        #         | ERROR: (gcloud.compute.start-iap-tunnel) Error while connecting [4033: 'not authorized'].
        #         | kex_exchange_identification: Connection closed by remote host
        #         | Connection closed by UNKNOWN port 65535
        #         |
        #         | Recommendation: To check for possible causes of SSH connectivity issues and get
        #         | recommendations, rerun the ssh command with the --troubleshoot option.
        #         |
        #         | gcloud compute ssh dataproc-cluster-gpu-w-0 --project=rapids-spark --zone=us-central1-a \
        #         --troubleshoot
        #         |
        #         | Or, to investigate an IAP tunneling issue:
        #         |
        #         | gcloud compute ssh dataproc-cluster-gpu-w-0-w-0 --project=project-id --zone=us-central1-a \
        #         --troubleshoot --tunnel-through-iap
        #         |
        #         | ERROR: (gcloud.compute.ssh) [/usr/bin/ssh] exited with return code [255].

        std_reg_expressions = [
            rf'Failure Running Rapids Tool \({self.get_tool_name().capitalize()}\)\.',
            r'Could not ssh to cluster',
            r'ERROR: \(gcloud\.compute\.ssh\)'
        ]
        self._run_tool_on_non_running_gpu_cluster(ut_dir, std_reg_expressions, submission_cluster=submission_cluster)
