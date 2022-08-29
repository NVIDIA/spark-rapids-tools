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
"""Diagnostic tool basic program."""

import json
import logging
import re
import sys

import fire
import pkg_resources

from spark_rapids_dataproc_tools.utilities import get_log_dict, run_cmd

# Setup logging
logger = logging.getLogger('diag')

consoleHandler = logging.StreamHandler(sys.stdout)
logFormatter = logging.Formatter("%(message)s")
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)

logger.setLevel(logging.INFO)


class Diagnostic:
    """Diagnostic tool basic class."""

    def __init__(self, debug=False):
        if debug:
            logging.config.dictConfig(get_log_dict({'debug': debug}))
            logger.setLevel(logging.DEBUG)

        # Diagnostic summary
        self.summary = {}
        self.nv_mvn_repo = 'https://repo1.maven.org/maven2/com/nvidia'

    def banner(func):   # pylint: disable=no-self-argument
        """Banner decorator."""
        def wrapper(self, *args, **kwargs):
            name = func.__name__    # pylint: disable=no-member
            logger.info(f'*** Running diagnostic function "{name}" ***')

            result = True
            try:
                func(self, *args, **kwargs)     # pylint: disable=not-callable

            except Exception as exception:    # pylint: disable=broad-except
                logger.error(f'Error: {exception}')
                result = False

            if result:
                logger.info(f'*** Check "{name}": PASS ***')
            else:
                logger.info(f'*** Check "{name}": FAIL ***')

            # Save result into summary
            if name in self.summary:
                self.summary[name] = any([result, self.summary[name]])
            else:
                self.summary[name] = result

        return wrapper

    def print_summary(self):
        """Print diagnostic summary."""
        logger.debug(self.summary)

        print('*'*80)

        result = all(self.summary.values())
        if result:
            print('Overall check result: PASS')
        else:
            print('Overall check result: FAIL')

        return result

    def run_local_cmd(self, cmd, check=True, capture=''):
        """Run command and check return code, capture output etc."""
        return run_cmd(cmd, check, capture)

    def run_cmd(self, cmd, check=True, capture=''):
        """Run command and check return code, capture output etc."""
        return self.run_local_cmd(cmd, check, capture)

    def all(self):
        """Diagnose all functions."""
        self.nv_driver()
        self.cuda_version()
        self.rapids_jar()
        self.deprecated_jar()
        self.spark()
        self.perf()

    @banner
    def nv_driver(self):
        """Diagnose nvidia driver."""
        self.run_cmd(['nvidia-smi'])

    @banner
    def cuda_version(self):
        """Diagnose cuda package version."""
        output = self.run_cmd(['cat', '/usr/local/cuda/version.json'], capture='stdout')
        version_info = json.loads(output)

        # Requirement: parse the major version and make sure it is >= 11
        # Sample output: { "cuda": { "name": "CUDA SDK", "version": "11.2.2" }...}
        cuda_ver = version_info.get('cuda', {}).get('version', None)
        if cuda_ver:
            major = cuda_ver.split('.')[0]
            logger.info(f'found cuda major version: {major}')

            if int(major) < 11:
                raise Exception(f'cuda major version: {major} < 11')

        else:
            raise Exception(f'not found cuda version: {output}')

    @banner
    def rapids_jar(self):
        """Diagnose rapids jar file."""
        output = self.run_cmd(['ls', '-altr', '$SPARK_HOME/jars/rapids-4-spark*.jar'], capture='stdout')

        # Requirement: only 1 version of RAPIDS Accelerator for Apache Spark jar is installed. No cudf jar existing
        # Sample output:
        # -rw-r--r-- 1 root root 412698840 Jun 17 13:51 /usr/lib/spark/jars/rapids-4-spark_2.12-22.06.0.jar
        if output.count('rapids-4-spark') > 1:
            raise Exception(f'found multiple rapids jar: {output}')

        matched = re.search('rapids-4-spark_(.+?).jar', output)
        if matched:
            version = matched.group(1)
            logger.info(f'found rapids jar version: {version}')

            self._import_pub_key()
            self._verify_rapids_jar_signature(version)

        else:
            raise Exception(f'not found rapids jar version: {output}')

    def _import_pub_key(self):
        """Import public key of sw-spark@nvidia.com."""
        # TODO: figure out how to handle pub key rotation
        pub_key_url = 'https://keys.openpgp.org/vks/v1/by-fingerprint/7A8A39909B9B202410C2A26F1D9E1285654392EF'

        # Download public key and import it
        self.run_cmd(['wget', pub_key_url, '-O', 'sw-spark@nvidia.com.pub'])
        self.run_cmd(['gpg', '--import', 'sw-spark@nvidia.com.pub'])

    def _verify_rapids_jar_signature(self, full_version):
        """Verify signature for rapids jar file."""
        # Requirement:
        #   verify signature: gpg --verify rapids-4-spark_2.12-22.08.0.jar.asc rapids-4-spark_2.12-22.08.0.jar
        # Construct asc file url, e.g.
        # https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/22.06.0/rapids-4-spark_2.12-22.06.0.jar.asc
        (scala_ver, rapids_ver) = full_version.split('-')
        asc_file = f'rapids-4-spark_{full_version}.jar.asc'

        asc_file_url = f'{self.nv_mvn_repo}/rapids-4-spark_{scala_ver}/{rapids_ver}/{asc_file}'

        # Download signature file
        self.run_cmd(['wget', asc_file_url, '-O', asc_file])

        # Verify signature for rapids jar file
        rapids_jar_path = f'$SPARK_HOME/jars/rapids-4-spark_{full_version}.jar'
        self.run_cmd(['gpg', '--verify', asc_file, rapids_jar_path])

    @banner
    def deprecated_jar(self):
        """Diagnose deprecated cudf jar file."""
        result = self.run_cmd(['ls', '-altr', '$SPARK_HOME/jars/cudf*.jar'], check=False, capture='stdout')

        if result.returncode == 0:
            if result.stdout.count(b'cudf') > 0:
                raise Exception(f'found cudf jar file: {result.stdout}')

    def run_spark_submit(self, options, capture='all'):
        """Run spark application via spark-submit command."""
        cmd = ['$SPARK_HOME/bin/spark-submit']
        cmd += options
        stdout, stderr = self.run_cmd(cmd, capture=capture)
        return stdout + stderr

    def get_diag_scripts(self, name):
        """Get diagnostic script path by name"""
        return pkg_resources.resource_filename(__name__, 'diag_scripts/' + name)

    def check_spark_output(self, output, run_type):
        """Check spark job output"""
        # Requirement: verify cpu/gpu output: cat hello-output |grep "run hello success"
        if output.count('run hello success') < 1:
            raise Exception(f'not found "run hello success" for {run_type} run from: {output}')

        # Requirement: verify gpu output: cat gpu-hello-output |grep "will run on GPU"
        if run_type == 'GPU' and output.count('will run on GPU') < 1:
            raise Exception(f'not found "will run on GPU" from: {output}')

    @banner
    def spark(self):
        """Diagnose spark."""
        cpu_opts = ['--master', 'yarn']
        cpu_opts += ['--conf', 'spark.rapids.sql.enabled=false']

        output = self.run_spark_submit(cpu_opts + [self.get_diag_scripts('hello_world.py')])
        self.check_spark_output(output, 'CPU')

        gpu_opts = ['--master', 'yarn']
        gpu_opts += ['--conf', 'spark.rapids.sql.enabled=true']
        gpu_opts += ['--conf', 'spark.task.resource.gpu.amount=0.5']
        gpu_opts += ['--conf', 'spark.rapids.sql.explain=ALL']

        output = self.run_spark_submit(gpu_opts + [self.get_diag_scripts('hello_world.py')])
        self.check_spark_output(output, 'GPU')

    def check_perf_output(self, output):
        """Check perf test output."""
        if output.count('run perf success') < 1:
            raise Exception(f'not found "run perf success" from: {output}')

        matched = re.search(r'Execution time:\s+(\d+.\d+)', output)
        if matched:
            return matched.group(1)

        raise Exception(f'not found "Execution time" from: {output}')

    def evaluate_perf_result(self, cpu_time, gpu_time):
        """Evaluate performance result between CPU & GPU runs."""
        # Requirement: The CPU "Execution time" should be 4-12 times than GPU depends on the range value
        # from 50000 to 100000, normally we can set 50000 by default because we do not want customers to
        # wait for 2+ mins during the checking works. We assume customer cluster’s spark default conf is
        # not updated by customer
        boost = float(cpu_time) / float(gpu_time)
        logger.info(f'Performance boost on GPU: {boost}')

        if boost < 3.0:
            logger.warning(f'performance boost on GPU is less than expected: {boost} < 3.0')

    @banner
    def perf(self):
        """Diagnose performance for a Spark job between CPU and GPU."""
        def run(opts):
            output = self.run_spark_submit(opts + [self.get_diag_scripts('perf.py')])
            return self.check_perf_output(output)

        cpu_opts = ['--master', 'yarn']
        cpu_opts += ['--conf', 'spark.rapids.sql.enabled=false']

        cpu_time = run(cpu_opts)
        logger.info(f'CPU execution time: {cpu_time}')

        gpu_opts = ['--master', 'yarn']
        gpu_opts += ['--conf', 'spark.rapids.sql.enabled=true']
        gpu_opts += ['--conf', 'spark.task.resource.gpu.amount=0.5']
        gpu_opts += ['--conf', 'spark.rapids.sql.explain=ALL']

        gpu_time = run(gpu_opts)
        logger.info(f'GPU execution time: {gpu_time}')

        self.evaluate_perf_result(cpu_time, gpu_time)


def main():
    """Main function."""
    fire.Fire(Diagnostic)


if __name__ == '__main__':
    main()
