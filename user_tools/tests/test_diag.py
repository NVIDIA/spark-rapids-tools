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

import json
from unittest.mock import patch, call, PropertyMock, ANY

import pytest  # pylint: disable=import-error
from cli_test_helpers import ArgvContext  # pylint: disable=import-error

from spark_rapids_dataproc_tools import dataproc_wrapper
from spark_rapids_dataproc_tools.csp.dataproc import Dataproc


# Mock cluster info for testing
mock_cluster_info = {
    'config': {
        'masterConfig': {
            'instanceNames': ['test-master'],
        },
        'workerConfig': {
            'instanceNames': ['test-worker'],
        },
        'gceClusterConfig': {
            'zoneUri': 'test-zone',
        }
    }
}


@patch('spark_rapids_dataproc_tools.csp.csp.CspBase.run_local_cmd')
@patch('spark_rapids_dataproc_tools.csp.dataproc.Dataproc.run_ssh_cmd')
def test_nv_driver(run_ssh_cmd, run_local_cmd):
    """Test run diagnostic nv_driver function."""
    # Mock cluster info
    run_local_cmd.return_value = json.dumps(mock_cluster_info)
    run_ssh_cmd.return_value = 'test nv_driver'

    with ArgvContext('spark_rapids_dataproc', 'diagnostic', '-c', 'test_cluster', '-r', 'test_region', 'nv_driver'):
        dataproc_wrapper.main()

    expected_local_cmd = [call(['gcloud', 'dataproc', 'clusters', 'describe', 'test_cluster', '--region=test_region'],
                          capture='stdout')]
    expected_ssh_cmd = [call(['nvidia-smi'], 'test-worker', True, '')]

    assert run_local_cmd.call_args_list == expected_local_cmd
    assert run_ssh_cmd.call_args_list == expected_ssh_cmd


@patch('spark_rapids_dataproc_tools.csp.csp.CspBase.run_local_cmd')
@patch('spark_rapids_dataproc_tools.csp.dataproc.Dataproc.run_ssh_cmd')
def test_cuda_version(run_ssh_cmd, run_local_cmd):
    """Test run diagnostic cuda_version function."""
    # Mock cluster info
    run_local_cmd.return_value = json.dumps(mock_cluster_info)

    # Mock positive cuda version
    run_ssh_cmd.return_value = json.dumps({
        'cuda': {
            'version': '11.2',
        }
    })

    with ArgvContext('spark_rapids_dataproc', 'diagnostic', '-c', 'test_cluster', '-r', 'test_region', 'cuda_version'):
        dataproc_wrapper.main()

    expected_local_cmd = [call(['gcloud', 'dataproc', 'clusters', 'describe', 'test_cluster', '--region=test_region'],
                          capture='stdout')]
    expected_ssh_cmd = [call(['cat', '/usr/local/cuda/version.json'], 'test-worker', True, 'stdout')]

    assert run_local_cmd.call_args_list == expected_local_cmd
    assert run_ssh_cmd.call_args_list == expected_ssh_cmd


@patch('spark_rapids_dataproc_tools.csp.csp.CspBase.run_local_cmd')
@patch('spark_rapids_dataproc_tools.csp.dataproc.Dataproc.run_ssh_cmd')
def test_cuda_version_failure(run_ssh_cmd, run_local_cmd, caplog, capsys):
    """Test run diagnostic cuda_version function for negative case."""
    # Mock cluster info
    run_local_cmd.return_value = json.dumps(mock_cluster_info)

    # Mock negative cuda version, i.e, major version < 11
    run_ssh_cmd.return_value = json.dumps({
        'cuda': {
            'version': '10.2',
        }
    })

    with pytest.raises(SystemExit):
        with ArgvContext('spark_rapids_dataproc', 'diagnostic', '-c', 'test_cluster', '-r', 'test_region',
                         'cuda_version'):
            dataproc_wrapper.main()

    expected_local_cmd = [call(['gcloud', 'dataproc', 'clusters', 'describe', 'test_cluster', '--region=test_region'],
                          capture='stdout')]
    expected_ssh_cmd = [call(['cat', '/usr/local/cuda/version.json'], 'test-worker', True, 'stdout')]

    assert run_local_cmd.call_args_list == expected_local_cmd
    assert run_ssh_cmd.call_args_list == expected_ssh_cmd
    assert 'cuda major version: 10 < 11' in caplog.text
    assert 'Check "cuda_version": FAIL' in caplog.text
    assert 'Overall check result: FAIL' in capsys.readouterr().out


@patch('spark_rapids_dataproc_tools.csp.csp.CspBase.run_local_cmd')
@patch('spark_rapids_dataproc_tools.csp.dataproc.Dataproc.run_ssh_cmd')
def test_rapids_jar(run_ssh_cmd, run_local_cmd):
    """Test run diagnostic rapids_jar function."""
    # Mock cluster info
    run_local_cmd.return_value = json.dumps(mock_cluster_info)

    # Mock positive rapids jar file
    run_ssh_cmd.return_value = 'rapids-4-spark_2.12-22.06.0.jar'

    with ArgvContext('spark_rapids_dataproc', 'diagnostic', '-c', 'test_cluster', '-r', 'test_region', 'rapids_jar'):
        dataproc_wrapper.main()

    expected_local_cmd = [call(['gcloud', 'dataproc', 'clusters', 'describe', 'test_cluster', '--region=test_region'],
                          capture='stdout')]
    expected_ssh_cmd = [
        call(['ls', '-altr', '$SPARK_HOME/jars/rapids-4-spark*.jar'], 'test-worker', True, 'stdout'),
        call(['wget', 'https://keys.openpgp.org/vks/v1/by-fingerprint/7A8A39909B9B202410C2A26F1D9E1285654392EF', '-O',
              'sw-spark@nvidia.com.pub'], 'test-worker', True, ''),
        call(['gpg', '--import', 'sw-spark@nvidia.com.pub'], 'test-worker', True, ''),
        call(['wget', 'https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.06.0/'
              'rapids-4-spark_2.12-22.06.0.jar.asc', '-O', 'rapids-4-spark_2.12-22.06.0.jar.asc'], 'test-worker', True,
             ''),
        call(['gpg', '--verify', 'rapids-4-spark_2.12-22.06.0.jar.asc',
              '$SPARK_HOME/jars/rapids-4-spark_2.12-22.06.0.jar'], 'test-worker', True, ''),
    ]

    assert run_local_cmd.call_args_list == expected_local_cmd
    assert run_ssh_cmd.call_args_list == expected_ssh_cmd


@patch('spark_rapids_dataproc_tools.csp.csp.CspBase.run_local_cmd')
@patch('spark_rapids_dataproc_tools.csp.dataproc.Dataproc.run_ssh_cmd')
def test_rapids_jar_failure(run_ssh_cmd, run_local_cmd, caplog, capsys):
    """Test run diagnostic rapids_jar function for negative case."""
    # Mock cluster info
    run_local_cmd.return_value = json.dumps(mock_cluster_info)

    # Mock multiple rapids jar files as negative case
    run_ssh_cmd.return_value = 'rapids-4-spark_2.12-22.06.0.jar;rapids-4-spark_2.12-22.08.0.jar'

    with pytest.raises(SystemExit):
        with ArgvContext('spark_rapids_dataproc', 'diagnostic', '-c', 'test_cluster', '-r', 'test_region',
                         'rapids_jar'):
            dataproc_wrapper.main()

    expected_local_cmd = [call(['gcloud', 'dataproc', 'clusters', 'describe', 'test_cluster', '--region=test_region'],
                          capture='stdout')]
    expected_ssh_cmd = [call(['ls', '-altr', '$SPARK_HOME/jars/rapids-4-spark*.jar'], 'test-worker', True, 'stdout')]

    assert run_local_cmd.call_args_list == expected_local_cmd
    assert run_ssh_cmd.call_args_list == expected_ssh_cmd
    assert 'found multiple rapids jar' in caplog.text
    assert 'Check "rapids_jar": FAIL' in caplog.text
    assert 'Overall check result: FAIL' in capsys.readouterr().out


@patch('spark_rapids_dataproc_tools.csp.csp.CspBase.run_local_cmd')
@patch('spark_rapids_dataproc_tools.csp.dataproc.Dataproc.run_ssh_cmd')
def test_deprecated_jar(run_ssh_cmd, run_local_cmd):
    """Test run diagnostic deprecated_jar function."""
    # Mock cluster info
    run_local_cmd.return_value = json.dumps(mock_cluster_info)

    # Mock not found cudf jar file
    run_ssh_cmd.return_value = PropertyMock(returncode=1)

    with ArgvContext('spark_rapids_dataproc', 'diagnostic', '-c', 'test_cluster', '-r', 'test_region',
                     'deprecated_jar'):
        dataproc_wrapper.main()

    expected_local_cmd = [call(['gcloud', 'dataproc', 'clusters', 'describe', 'test_cluster', '--region=test_region'],
                          capture='stdout')]
    expected_ssh_cmd = [call(['ls', '-altr', '$SPARK_HOME/jars/cudf*.jar'], 'test-worker', False, 'stdout')]

    assert run_local_cmd.call_args_list == expected_local_cmd
    assert run_ssh_cmd.call_args_list == expected_ssh_cmd


@patch('spark_rapids_dataproc_tools.csp.csp.CspBase.run_local_cmd')
@patch('spark_rapids_dataproc_tools.csp.dataproc.Dataproc.run_ssh_cmd')
def test_deprecated_jar_failure(run_ssh_cmd, run_local_cmd, caplog, capsys):
    """Test run diagnostic deprecated_jar function for negative case."""
    # Mock cluster info
    run_local_cmd.return_value = json.dumps(mock_cluster_info)

    # Mock cudf jar existence
    run_ssh_cmd.return_value = PropertyMock(returncode=0, stdout=b'/usr/lib/spark/jars/cudf-22.06.0-cuda11.jar')

    with pytest.raises(SystemExit):
        with ArgvContext('spark_rapids_dataproc', 'diagnostic', '-c', 'test_cluster', '-r', 'test_region',
                         'deprecated_jar'):
            dataproc_wrapper.main()

    expected_local_cmd = [call(['gcloud', 'dataproc', 'clusters', 'describe', 'test_cluster', '--region=test_region'],
                          capture='stdout')]
    expected_ssh_cmd = [call(['ls', '-altr', '$SPARK_HOME/jars/cudf*.jar'], 'test-worker', False, 'stdout')]

    assert run_local_cmd.call_args_list == expected_local_cmd
    assert run_ssh_cmd.call_args_list == expected_ssh_cmd
    assert 'found cudf jar file' in caplog.text
    assert 'Check "deprecated_jar": FAIL' in caplog.text
    assert 'Overall check result: FAIL' in capsys.readouterr().out


@patch('spark_rapids_dataproc_tools.csp.csp.CspBase.run_local_cmd')
@patch('spark_rapids_dataproc_tools.csp.dataproc.Dataproc.run_ssh_cmd')
def test_spark(run_ssh_cmd, run_local_cmd):
    """Test run diagnostic spark function."""
    # Mock cluster info
    run_local_cmd.return_value = json.dumps(mock_cluster_info)

    # Mock spark job output
    run_ssh_cmd.return_value = ('run hello success; will run on GPU', '')

    with ArgvContext('spark_rapids_dataproc', 'diagnostic', '-c', 'test_cluster', '-r', 'test_region', 'spark'):
        dataproc_wrapper.main()

    expected_local_cmd = [
        call(['gcloud', 'dataproc', 'clusters', 'describe', 'test_cluster', '--region=test_region'],
             capture='stdout'),
        call(['gcloud', 'compute', 'scp', '--zone', 'test-zone', ANY, 'test-master:hello_world.py']),
        call(['gcloud', 'compute', 'scp', '--zone', 'test-zone', ANY, 'test-master:hello_world.py']),
    ]
    expected_ssh_cmd = [
        call(['$SPARK_HOME/bin/spark-submit', '--master', 'yarn', '--conf', 'spark.rapids.sql.enabled=false',
              'hello_world.py'], 'test-master', True, 'all'),
        call(['$SPARK_HOME/bin/spark-submit', '--master', 'yarn', '--conf', 'spark.rapids.sql.enabled=true', '--conf',
              'spark.task.resource.gpu.amount=0.5', '--conf', 'spark.rapids.sql.explain=ALL', 'hello_world.py'],
             'test-master', True, 'all'),
    ]

    assert run_local_cmd.call_args_list == expected_local_cmd
    assert run_ssh_cmd.call_args_list == expected_ssh_cmd


@patch('spark_rapids_dataproc_tools.csp.csp.CspBase.run_local_cmd')
@patch('spark_rapids_dataproc_tools.csp.dataproc.Dataproc.run_ssh_cmd')
def test_spark_cpu_failure(run_ssh_cmd, run_local_cmd, caplog, capsys):
    """Test run diagnostic spark function for cpu negative case."""
    # Mock cluster info
    run_local_cmd.return_value = json.dumps(mock_cluster_info)

    # Mock spark job output
    run_ssh_cmd.return_value = ('', '')

    with pytest.raises(SystemExit):
        with ArgvContext('spark_rapids_dataproc', 'diagnostic', '-c', 'test_cluster', '-r', 'test_region',
                         'spark'):
            dataproc_wrapper.main()

    expected_local_cmd = [
        call(['gcloud', 'dataproc', 'clusters', 'describe', 'test_cluster', '--region=test_region'],
             capture='stdout'),
        call(['gcloud', 'compute', 'scp', '--zone', 'test-zone', ANY, 'test-master:hello_world.py']),
    ]
    expected_ssh_cmd = [
        call(['$SPARK_HOME/bin/spark-submit', '--master', 'yarn', '--conf', 'spark.rapids.sql.enabled=false',
              'hello_world.py'], 'test-master', True, 'all'),
    ]

    assert run_local_cmd.call_args_list == expected_local_cmd
    assert run_ssh_cmd.call_args_list == expected_ssh_cmd
    assert 'not found "run hello success"' in caplog.text
    assert 'Check "spark": FAIL' in caplog.text
    assert 'Overall check result: FAIL' in capsys.readouterr().out


@patch('spark_rapids_dataproc_tools.csp.csp.CspBase.run_local_cmd')
@patch('spark_rapids_dataproc_tools.csp.dataproc.Dataproc.run_ssh_cmd')
def test_spark_gpu_failure(run_ssh_cmd, run_local_cmd, caplog, capsys):
    """Test run diagnostic spark function for gpu negative case."""
    # Mock cluster info
    run_local_cmd.return_value = json.dumps(mock_cluster_info)

    # Mock spark job output
    run_ssh_cmd.return_value = ('run hello success', '')

    with pytest.raises(SystemExit):
        with ArgvContext('spark_rapids_dataproc', 'diagnostic', '-c', 'test_cluster', '-r', 'test_region',
                         'spark'):
            dataproc_wrapper.main()

    expected_local_cmd = [
        call(['gcloud', 'dataproc', 'clusters', 'describe', 'test_cluster', '--region=test_region'],
             capture='stdout'),
        call(['gcloud', 'compute', 'scp', '--zone', 'test-zone', ANY, 'test-master:hello_world.py']),
        call(['gcloud', 'compute', 'scp', '--zone', 'test-zone', ANY, 'test-master:hello_world.py']),
    ]
    expected_ssh_cmd = [
        call(['$SPARK_HOME/bin/spark-submit', '--master', 'yarn', '--conf', 'spark.rapids.sql.enabled=false',
              'hello_world.py'], 'test-master', True, 'all'),
        call(['$SPARK_HOME/bin/spark-submit', '--master', 'yarn', '--conf', 'spark.rapids.sql.enabled=true', '--conf',
              'spark.task.resource.gpu.amount=0.5', '--conf', 'spark.rapids.sql.explain=ALL', 'hello_world.py'],
             'test-master', True, 'all'),
    ]

    assert run_local_cmd.call_args_list == expected_local_cmd
    assert run_ssh_cmd.call_args_list == expected_ssh_cmd
    assert 'not found "will run on GPU"' in caplog.text
    assert 'Check "spark": FAIL' in caplog.text
    assert 'Overall check result: FAIL' in capsys.readouterr().out


@patch('spark_rapids_dataproc_tools.csp.csp.CspBase.run_local_cmd')
def test_perf(run_local_cmd):
    """Test run diagnostic perf function."""
    # Mock cluster info
    run_local_cmd.return_value = json.dumps(mock_cluster_info)

    # Mock spark job output
    return_value = [('run perf success;Execution time: 4.0', ''), ('run perf success;Execution time: 1.0', '')]

    with patch.object(Dataproc, 'run_ssh_cmd', side_effect=return_value) as run_ssh_cmd:

        with ArgvContext('spark_rapids_dataproc', 'diagnostic', '-c', 'test_cluster', '-r', 'test_region', 'perf'):
            dataproc_wrapper.main()

        expected_local_cmd = [
            call(['gcloud', 'dataproc', 'clusters', 'describe', 'test_cluster', '--region=test_region'],
                 capture='stdout'),
            call(['gcloud', 'compute', 'scp', '--zone', 'test-zone', ANY, 'test-master:perf.py']),
            call(['gcloud', 'compute', 'scp', '--zone', 'test-zone', ANY, 'test-master:perf.py']),
        ]
        expected_ssh_cmd = [
            call(['$SPARK_HOME/bin/spark-submit', '--master', 'yarn', '--conf', 'spark.rapids.sql.enabled=false',
                  'perf.py'], 'test-master', True, 'all'),
            call(['$SPARK_HOME/bin/spark-submit', '--master', 'yarn', '--conf', 'spark.rapids.sql.enabled=true',
                  '--conf', 'spark.task.resource.gpu.amount=0.5', '--conf', 'spark.rapids.sql.explain=ALL',
                  'perf.py'], 'test-master', True, 'all'),
        ]

        assert run_local_cmd.call_args_list == expected_local_cmd
        assert run_ssh_cmd.call_args_list == expected_ssh_cmd


@pytest.mark.parametrize('mock_output,expected',
                         [('', 'run perf success'),
                          ('run perf success', 'Execution time')])
@patch('spark_rapids_dataproc_tools.csp.csp.CspBase.run_local_cmd')
def test_perf_cpu_failure(run_local_cmd, mock_output, expected, caplog, capsys):
    """Test run diagnostic perf function for cpu negative case."""
    # Mock cluster info
    run_local_cmd.return_value = json.dumps(mock_cluster_info)

    # Mock spark job output
    return_value = [(mock_output, '')]

    with patch.object(Dataproc, 'run_ssh_cmd', side_effect=return_value) as run_ssh_cmd:

        with pytest.raises(SystemExit):
            with ArgvContext('spark_rapids_dataproc', 'diagnostic', '-c', 'test_cluster', '-r', 'test_region', 'perf'):
                dataproc_wrapper.main()

        expected_local_cmd = [
            call(['gcloud', 'dataproc', 'clusters', 'describe', 'test_cluster', '--region=test_region'],
                 capture='stdout'),
            call(['gcloud', 'compute', 'scp', '--zone', 'test-zone', ANY, 'test-master:perf.py']),
        ]
        expected_ssh_cmd = [
            call(['$SPARK_HOME/bin/spark-submit', '--master', 'yarn', '--conf', 'spark.rapids.sql.enabled=false',
                  'perf.py'], 'test-master', True, 'all'),
        ]

        assert run_local_cmd.call_args_list == expected_local_cmd
        assert run_ssh_cmd.call_args_list == expected_ssh_cmd
        assert f'not found "{expected}"' in caplog.text
        assert 'Check "perf": FAIL' in caplog.text
        assert 'Overall check result: FAIL' in capsys.readouterr().out


@pytest.mark.parametrize('mock_input,expected',
                         [('', 'run perf success'),
                          ('run perf success', 'Execution time')])
@patch('spark_rapids_dataproc_tools.csp.csp.CspBase.run_local_cmd')
def test_perf_gpu_failure(run_local_cmd, mock_input, expected, caplog, capsys):
    """Test run diagnostic perf function for gpu negative case."""
    # Mock cluster info
    run_local_cmd.return_value = json.dumps(mock_cluster_info)

    # Mock spark job output
    return_value = [('run perf success;Execution time: 4.0', ''), (mock_input, '')]

    with patch.object(Dataproc, 'run_ssh_cmd', side_effect=return_value) as run_ssh_cmd:

        with pytest.raises(SystemExit):
            with ArgvContext('spark_rapids_dataproc', 'diagnostic', '-c', 'test_cluster', '-r', 'test_region', 'perf'):
                dataproc_wrapper.main()

        expected_local_cmd = [
            call(['gcloud', 'dataproc', 'clusters', 'describe', 'test_cluster', '--region=test_region'],
                 capture='stdout'),
            call(['gcloud', 'compute', 'scp', '--zone', 'test-zone', ANY, 'test-master:perf.py']),
            call(['gcloud', 'compute', 'scp', '--zone', 'test-zone', ANY, 'test-master:perf.py']),
        ]
        expected_ssh_cmd = [
            call(['$SPARK_HOME/bin/spark-submit', '--master', 'yarn', '--conf', 'spark.rapids.sql.enabled=false',
                  'perf.py'], 'test-master', True, 'all'),
            call(['$SPARK_HOME/bin/spark-submit', '--master', 'yarn', '--conf', 'spark.rapids.sql.enabled=true',
                  '--conf', 'spark.task.resource.gpu.amount=0.5', '--conf', 'spark.rapids.sql.explain=ALL',
                  'perf.py'], 'test-master', True, 'all'),
        ]

        assert run_local_cmd.call_args_list == expected_local_cmd
        assert run_ssh_cmd.call_args_list == expected_ssh_cmd
        assert f'not found "{expected}"' in caplog.text
        assert 'Check "perf": FAIL' in caplog.text
        assert 'Overall check result: FAIL' in capsys.readouterr().out


@patch('spark_rapids_dataproc_tools.csp.csp.CspBase.run_local_cmd')
def test_perf_failure(run_local_cmd, caplog, capsys):
    """Test run diagnostic perf function for negative case."""
    # Mock cluster info
    run_local_cmd.return_value = json.dumps(mock_cluster_info)

    # Mock spark job output
    return_value = [('run perf success;Execution time: 2.0', ''), ('run perf success;Execution time: 1.0', '')]

    with patch.object(Dataproc, 'run_ssh_cmd', side_effect=return_value) as run_ssh_cmd:

        with ArgvContext('spark_rapids_dataproc', 'diagnostic', '-c', 'test_cluster', '-r', 'test_region', 'perf'):
            dataproc_wrapper.main()

        expected_local_cmd = [
            call(['gcloud', 'dataproc', 'clusters', 'describe', 'test_cluster', '--region=test_region'],
                 capture='stdout'),
            call(['gcloud', 'compute', 'scp', '--zone', 'test-zone', ANY, 'test-master:perf.py']),
            call(['gcloud', 'compute', 'scp', '--zone', 'test-zone', ANY, 'test-master:perf.py']),
        ]
        expected_ssh_cmd = [
            call(['$SPARK_HOME/bin/spark-submit', '--master', 'yarn', '--conf', 'spark.rapids.sql.enabled=false',
                  'perf.py'], 'test-master', True, 'all'),
            call(['$SPARK_HOME/bin/spark-submit', '--master', 'yarn', '--conf', 'spark.rapids.sql.enabled=true',
                  '--conf', 'spark.task.resource.gpu.amount=0.5', '--conf', 'spark.rapids.sql.explain=ALL',
                  'perf.py'], 'test-master', True, 'all'),
        ]

        assert run_local_cmd.call_args_list == expected_local_cmd
        assert run_ssh_cmd.call_args_list == expected_ssh_cmd
        assert 'performance boost on GPU is less than expected: 2.0 < 3.0' in caplog.text
        assert 'Check "perf": PASS' in caplog.text
        assert 'Overall check result: PASS' in capsys.readouterr().out


def test_spark_job():
    """Test run diagnostic spark_job function."""
    return_value = ['run hello success', 'run hello success;will run on GPU']

    with patch.object(Dataproc, 'run_local_cmd', side_effect=return_value) as run_local_cmd:

        with ArgvContext('spark_rapids_dataproc', 'diagnostic', '-c', 'test_cluster', '-r', 'test_region', 'spark_job'):
            dataproc_wrapper.main()

        expected_local_cmd = [
            call(['gcloud', 'dataproc', 'jobs', 'submit', 'pyspark', '--cluster=test_cluster', '--region=test_region',
                  '--properties', "spark.rapids.sql.enabled='false'", ANY], capture='stderr'),
            call(['gcloud', 'dataproc', 'jobs', 'submit', 'pyspark', '--cluster=test_cluster', '--region=test_region',
                  '--properties', "spark.rapids.sql.enabled='true',spark.task.resource.gpu.amount='0.5',"
                  "spark.rapids.sql.explain='ALL'", ANY], capture='stderr'),
        ]

        assert run_local_cmd.call_args_list == expected_local_cmd


@patch('spark_rapids_dataproc_tools.csp.csp.CspBase.run_local_cmd')
def test_spark_job_cpu_failure(run_local_cmd, caplog, capsys):
    """Test run diagnostic spark_job function for cpu failure case."""
    # Mock spark job output
    run_local_cmd.return_value = ''

    with pytest.raises(SystemExit):
        with ArgvContext('spark_rapids_dataproc', 'diagnostic', '-c', 'test_cluster', '-r', 'test_region',
                         'spark_job'):
            dataproc_wrapper.main()

    expected_local_cmd = [
        call(['gcloud', 'dataproc', 'jobs', 'submit', 'pyspark', '--cluster=test_cluster', '--region=test_region',
              '--properties', "spark.rapids.sql.enabled='false'", ANY], capture='stderr'),
    ]

    assert run_local_cmd.call_args_list == expected_local_cmd
    assert 'not found "run hello success"' in caplog.text
    assert 'Check "spark_job": FAIL' in caplog.text
    assert 'Overall check result: FAIL' in capsys.readouterr().out


@patch('spark_rapids_dataproc_tools.csp.csp.CspBase.run_local_cmd')
def test_spark_job_gpu_failure(run_local_cmd, caplog, capsys):
    """Test run diagnostic spark_job function for gpu failure case."""
    # Mock spark job output
    run_local_cmd.return_value = 'run hello success'

    with pytest.raises(SystemExit):
        with ArgvContext('spark_rapids_dataproc', 'diagnostic', '-c', 'test_cluster', '-r', 'test_region',
                         'spark_job'):
            dataproc_wrapper.main()

    expected_local_cmd = [
        call(['gcloud', 'dataproc', 'jobs', 'submit', 'pyspark', '--cluster=test_cluster', '--region=test_region',
              '--properties', "spark.rapids.sql.enabled='false'", ANY], capture='stderr'),
        call(['gcloud', 'dataproc', 'jobs', 'submit', 'pyspark', '--cluster=test_cluster', '--region=test_region',
              '--properties', "spark.rapids.sql.enabled='true',spark.task.resource.gpu.amount='0.5',"
              "spark.rapids.sql.explain='ALL'", ANY], capture='stderr'),
    ]

    assert run_local_cmd.call_args_list == expected_local_cmd
    assert 'not found "will run on GPU"' in caplog.text
    assert 'Check "spark_job": FAIL' in caplog.text
    assert 'Overall check result: FAIL' in capsys.readouterr().out


def test_perf_job():
    """Test run diagnostic perf_job function."""
    # Mock spark job output
    return_value = ['run perf success;Execution time: 4.0', 'run perf success;Execution time: 1.0']

    with patch.object(Dataproc, 'run_local_cmd', side_effect=return_value) as run_local_cmd:

        with ArgvContext('spark_rapids_dataproc', 'diagnostic', '-c', 'test_cluster', '-r', 'test_region', 'perf_job'):
            dataproc_wrapper.main()

        expected_local_cmd = [
            call(['gcloud', 'dataproc', 'jobs', 'submit', 'pyspark', '--cluster=test_cluster', '--region=test_region',
                  '--properties', "spark.rapids.sql.enabled='false'", ANY], capture='stderr'),
            call(['gcloud', 'dataproc', 'jobs', 'submit', 'pyspark', '--cluster=test_cluster', '--region=test_region',
                  '--properties', "spark.rapids.sql.enabled='true',spark.task.resource.gpu.amount='0.5',"
                  "spark.rapids.sql.explain='ALL'", ANY], capture='stderr'),
        ]

        assert run_local_cmd.call_args_list == expected_local_cmd


@pytest.mark.parametrize('mock_input,expected',
                         [('', 'run perf success'),
                          ('run perf success', 'Execution time')])
def test_perf_job_cpu_failure(mock_input, expected, caplog, capsys):
    """Test run diagnostic perf_job function for cpu failure case."""
    # Mock spark job output
    return_value = [mock_input]

    with patch.object(Dataproc, 'run_local_cmd', side_effect=return_value) as run_local_cmd:

        with pytest.raises(SystemExit):
            with ArgvContext('spark_rapids_dataproc', 'diagnostic', '-c', 'test_cluster', '-r', 'test_region',
                             'perf_job'):
                dataproc_wrapper.main()

        expected_local_cmd = [
            call(['gcloud', 'dataproc', 'jobs', 'submit', 'pyspark', '--cluster=test_cluster', '--region=test_region',
                  '--properties', "spark.rapids.sql.enabled='false'", ANY], capture='stderr'),
        ]

        assert run_local_cmd.call_args_list == expected_local_cmd
        assert f'not found "{expected}"' in caplog.text
        assert 'Check "perf_job": FAIL' in caplog.text
        assert 'Overall check result: FAIL' in capsys.readouterr().out


@pytest.mark.parametrize('mock_input,expected',
                         [('', 'run perf success'),
                          ('run perf success', 'Execution time')])
def test_perf_job_gpu_failure(mock_input, expected, caplog, capsys):
    """Test run diagnostic perf_job function for gpu failure case."""
    # Mock spark job output
    return_value = ['run perf success;Execution time: 4.0', mock_input]

    with patch.object(Dataproc, 'run_local_cmd', side_effect=return_value) as run_local_cmd:

        with pytest.raises(SystemExit):
            with ArgvContext('spark_rapids_dataproc', 'diagnostic', '-c', 'test_cluster', '-r', 'test_region',
                             'perf_job'):
                dataproc_wrapper.main()

        expected_local_cmd = [
            call(['gcloud', 'dataproc', 'jobs', 'submit', 'pyspark', '--cluster=test_cluster', '--region=test_region',
                  '--properties', "spark.rapids.sql.enabled='false'", ANY], capture='stderr'),
            call(['gcloud', 'dataproc', 'jobs', 'submit', 'pyspark', '--cluster=test_cluster', '--region=test_region',
                  '--properties', "spark.rapids.sql.enabled='true',spark.task.resource.gpu.amount='0.5',"
                  "spark.rapids.sql.explain='ALL'", ANY], capture='stderr'),
        ]

        assert run_local_cmd.call_args_list == expected_local_cmd
        assert f'not found "{expected}"' in caplog.text
        assert 'Check "perf_job": FAIL' in caplog.text
        assert 'Overall check result: FAIL' in capsys.readouterr().out


def test_perf_job_failure(caplog, capsys):
    """Test run diagnostic perf_job function for failure case."""
    # Mock spark job output
    return_value = ['run perf success;Execution time: 2.0', 'run perf success;Execution time: 1.0']

    with patch.object(Dataproc, 'run_local_cmd', side_effect=return_value) as run_local_cmd:

        with ArgvContext('spark_rapids_dataproc', 'diagnostic', '-c', 'test_cluster', '-r', 'test_region',
                         'perf_job'):
            dataproc_wrapper.main()

        expected_local_cmd = [
            call(['gcloud', 'dataproc', 'jobs', 'submit', 'pyspark', '--cluster=test_cluster', '--region=test_region',
                  '--properties', "spark.rapids.sql.enabled='false'", ANY], capture='stderr'),
            call(['gcloud', 'dataproc', 'jobs', 'submit', 'pyspark', '--cluster=test_cluster', '--region=test_region',
                  '--properties', "spark.rapids.sql.enabled='true',spark.task.resource.gpu.amount='0.5',"
                  "spark.rapids.sql.explain='ALL'", ANY], capture='stderr'),
        ]

        assert run_local_cmd.call_args_list == expected_local_cmd
        assert 'performance boost on GPU is less than expected: 2.0 < 3.0' in caplog.text
        assert 'Check "perf_job": PASS' in caplog.text
        assert 'Overall check result: PASS' in capsys.readouterr().out
