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
"""Test basic functions."""
import os
import subprocess

import pytest  # pylint: disable=import-error

from conftest import assert_patterns_in_output, get_wrapper_work_dir


def test_help():
    """Test help."""
    result = subprocess.check_output(['spark_rapids_dataproc', '--help'], stderr=subprocess.STDOUT)

    expected = '''INFO: Showing help with the command 'spark_rapids_dataproc -- --help'.

NAME
    spark_rapids_dataproc - A wrapper script to run Rapids tools (Qualification, Profiling, and Bootstrap) tools \
on DataProc. Disclaimer: Estimates provided by the tools are based on the currently supported "SparkPlan" or \
"Executor Nodes" used in the application. It currently does not handle all the expressions or datatypes used. \
The pricing estimate does not take into considerations: 1- Sustained Use discounts 2- Cost of on-demand VMs

SYNOPSIS
    spark_rapids_dataproc -

DESCRIPTION
    Run one of the following commands:
    :qualification args
    :profiling args
    :bootstrap args

    For more details on each command: run qualification --help
'''

    assert expected in result.decode('utf-8')


def test_help_diag():
    """Test help."""
    result = subprocess.check_output(['spark_rapids_dataproc', 'diagnostic', '--help'], stderr=subprocess.STDOUT)

    expected = '''INFO: Showing help with the command 'spark_rapids_dataproc diagnostic -- --help'.

NAME
    spark_rapids_dataproc diagnostic - Run diagnostic on local environment or remote Dataproc cluster, \
such as check installed NVIDIA driver, CUDA toolkit, RAPIDS Accelerator for Apache Spark jar etc.

SYNOPSIS
    spark_rapids_dataproc diagnostic CLUSTER REGION <flags>

DESCRIPTION
    Run diagnostic on local environment or remote Dataproc cluster, such as check installed NVIDIA driver, \
CUDA toolkit, RAPIDS Accelerator for Apache Spark jar etc.

POSITIONAL ARGUMENTS
    CLUSTER
        Type: str
        Name of the Dataproc cluster.
    REGION
        Type: str
        Region of Dataproc cluster (e.g. us-central1)

FLAGS
    --func=FUNC
        Type: str
        Default: 'all'
        Diagnostic function to run. Available functions: 'nv_driver': dump NVIDIA driver info via command \
`nvidia-smi`, 'cuda_version': check if CUDA toolkit major version >= 11.0, 'rapids_jar': check if only single \
RAPIDS Accelerator for Apache Spark jar is installed and verify its signature, 'deprecated_jar': check if \
deprecated (cudf) jar is installed. I.e. should no cudf jar starting with RAPIDS Accelerator for \
Apache Spark 22.08, 'spark': run a Hello-world Spark Application on CPU and GPU, 'perf': performance test \
for a Spark job between CPU and GPU, 'spark_job': run a Hello-world Spark Application on CPU and GPU via \
Dataproc job interface, 'perf_job': performance test for a Spark job between CPU and GPU via Dataproc job interface
    --debug=DEBUG
        Type: bool
        Default: False
        True or False to enable verbosity

NOTES
    You can also use flags syntax for POSITIONAL ARGUMENTS
'''

    assert expected in result.decode('utf-8')


def test_diag_no_args():
    """Test diagnostic without arguments."""
    # pylint: disable=subprocess-run-check
    result = subprocess.run(['spark_rapids_dataproc', 'diagnostic'], stderr=subprocess.PIPE)
    # pylint: enable=subprocess-run-check

    assert result.returncode != 0
    assert b'ERROR: The function received no value for the required argument' in result.stderr


@pytest.mark.parametrize('tool_name', ['qualification', 'profiling', 'bootstrap'])
def test_fail_non_existing_cluster(tool_name, tmp_path, capfd):
    """
    Rin the wrapper tool on non-existing cluster.
    :param tool_name: the name of the tool (profiling/qualification/bootstrap)
    :param tmp_path: fixture which will provide a temporary directory unique to the test invocation,
           created in the base temporary directory.
    :param capfd: fixture in which all writes going to the operating system file descriptors 1 and 2 will be captured.
    """

    # Expected output running on invalid cluster
    # Failure Running Rapids Tool (Tool_name).
    # \tCould not pull Cluster description region:us-central1, dataproc-test-cluster
    # \tRun Terminated with error.
    # \tError invoking CMD <gcloud dataproc clusters describe dataproc-test-cluster --region=us-central1>:
    # \t| ERROR: (gcloud.dataproc.clusters.describe) NOT_FOUND: Not found: \
    # Cluster projects/project-id/regions/us-central1/clusters/dataproc-test-cluster

    cluster_name = 'dataproc-test-non-existing-cluster'
    std_reg_expressions = [
        rf'Failure Running Rapids Tool \({tool_name.capitalize()}\)\.',
        r'Could not pull Cluster description',
        r'Run Terminated with error\.',
        r'ERROR: \(gcloud\.dataproc\.clusters\.describe\) NOT_FOUND: Not found:',
    ]
    # Run the actual test
    wrapper_args_arr = [
        '--region=us-central1',
        f'--cluster={cluster_name}',
        f'--output_folder={tmp_path}'
    ]
    wrapper_args = ' '.join(wrapper_args_arr)
    # pylint: disable=subprocess-run-check
    c = subprocess.run(f'spark_rapids_dataproc {tool_name} {wrapper_args}', shell=True, text=True)
    # pylint: enable=subprocess-run-check
    assert c.returncode != 0, f'Running {tool_name.capitalize()} on non-existing cluster should fail'
    captured_output = capfd.readouterr()
    assert_patterns_in_output(std_reg_expressions, captured_output.out)
    assert not os.path.exists(get_wrapper_work_dir(tool_name, tmp_path))
