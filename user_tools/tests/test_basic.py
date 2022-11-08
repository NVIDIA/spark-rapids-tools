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

import subprocess


def test_help():
    """Test help."""
    result = subprocess.check_output(['spark_rapids_dataproc', '--help'], stderr=subprocess.STDOUT)

    expected = '''INFO: Showing help with the command 'spark_rapids_dataproc -- --help'.

\x1B[1mNAME\x1B[0m
    spark_rapids_dataproc - A wrapper script to run Rapids tools (Qualification, Profiling, and Bootstrap) tools on \
DataProc. Disclaimer: Estimates provided by the tools are based on the currently supported "SparkPlan" or "Executor \
Nodes" used in the application. It currently does not handle all the expressions or datatypes used. The pricing \
estimate does not take into considerations: 1- Sustained Use discounts 2- Cost of on-demand VMs

\x1B[1mSYNOPSIS\x1B[0m
    spark_rapids_dataproc -

\x1B[1mDESCRIPTION\x1B[0m
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

    expected = '''\x1B[1mNAME\x1B[0m
    spark_rapids_dataproc diagnostic - Run diagnostic on local environment or remote Dataproc cluster, such as check \
installed NVIDIA driver, CUDA toolkit, RAPIDS Accelerator for Apache Spark jar etc.

\x1B[1mSYNOPSIS\x1B[0m
    spark_rapids_dataproc diagnostic \x1B[4mCLUSTER\x1B[0m \x1B[4mREGION\x1B[0m <flags>

\x1B[1mDESCRIPTION\x1B[0m
    Run diagnostic on local environment or remote Dataproc cluster, such as check installed NVIDIA driver, CUDA \
toolkit, RAPIDS Accelerator for Apache Spark jar etc.

\x1B[1mPOSITIONAL ARGUMENTS\x1B[0m
    \x1B[1m\x1B[4mCLUSTER\x1B[0m\x1B[0m
        Type: str
        Name of the Dataproc cluster
    \x1B[1m\x1B[4mREGION\x1B[0m\x1B[0m
        Type: str
        Region of Dataproc cluster (e.g. us-central1)

\x1B[1mFLAGS\x1B[0m
    --func=\x1B[4mFUNC\x1B[0m
        Type: str
        Default: 'all'
        Diagnostic function to run. Available functions: 'nv_driver': dump NVIDIA driver info via command \
`nvidia-smi`, 'cuda_version': check if CUDA toolkit major version >= 11.0, 'rapids_jar': check if only single RAPIDS Accelerator for \
Apache Spark jar is installed and verify its signature, 'deprecated_jar': check if deprecated (cudf) jar is installed. I.e. \
should no cudf jar starting with RAPIDS Accelerator for Apache Spark 22.08, 'spark': run a Hello-world Spark Application on CPU and GPU, \
'perf': performance test for a Spark job between CPU and GPU, 'spark_job': run a Hello-world Spark Application on CPU \
and GPU via Dataproc job interface, 'perf_job': performance test for a Spark job between CPU and GPU via Dataproc job \
interface
    --debug=\x1B[4mDEBUG\x1B[0m
        Type: bool
        Default: False
        True or False to enable verbosity

\x1B[1mNOTES\x1B[0m
    You can also use flags syntax for POSITIONAL ARGUMENTS
'''

    assert expected in result.decode('utf-8')


def test_diag_no_args():
    """Test diagnostic without arguments."""
    # pylint: disable=subprocess-run-check
    result = subprocess.run(['spark_rapids_dataproc', 'diagnostic'], stderr=subprocess.PIPE)
    # pylint: enable=subprocess-run-check

    assert result.returncode != 0
    assert b'ERROR: \x1b[0mThe function received no value' in result.stderr
