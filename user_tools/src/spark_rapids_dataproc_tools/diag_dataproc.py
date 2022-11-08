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
"""Diagnostic tool for Dataproc."""

import logging
import os

import fire

from spark_rapids_dataproc_tools.diag import Diagnostic
from spark_rapids_dataproc_tools.csp import new_csp

logger = logging.getLogger('diag_dataproc')


class DiagDataproc(Diagnostic):
    """Diagnostic tool for Dataproc."""

    def __init__(self, cluster_name, region, debug=False):
        super().__init__(debug)

        self.cluster = new_csp('dataproc', args={'cluster': cluster_name, 'region': region})

    def on(node):  # pylint: disable=invalid-name,no-self-argument
        """On decorator."""
        def inner_decorator(func):
            def wrapper(self, *args, **kwargs):
                for i in self.cluster.get_nodes(node):

                    def run(cmd, check=True, capture=''):
                        return self.cluster.run_ssh_cmd(cmd, i, check, capture)  # pylint: disable=cell-var-from-loop

                    self.run_cmd = run
                    func(self, *args, **kwargs)

            return wrapper
        return inner_decorator

    def all(self):
        """Diagnose all functions."""
        self.nv_driver()
        self.cuda_version()
        self.rapids_jar()
        self.deprecated_jar()

        # Include diagnostic functions that leveraged Dataproc job interface
        self.spark_job()
        self.perf_job()

    @on('workers')
    def nv_driver(self):
        """Diagnose nvidia driver."""
        return super().nv_driver()

    @on('workers')
    def cuda_version(self):
        """Diagnose cuda package version."""
        return super().cuda_version()

    @on('workers')
    def rapids_jar(self):
        """Diagnose rapids jar file."""
        return super().rapids_jar()

    @on('workers')
    def deprecated_jar(self):
        """Diagnose deprecated cudf jar file."""
        return super().deprecated_jar()

    def get_diag_scripts(self, name):
        """Get diagnostic script path by name"""
        script_path = super().get_diag_scripts(name)
        dest = os.path.basename(script_path)

        master = self.cluster.get_nodes('master')
        self.cluster.run_scp_cmd(script_path, dest, master[0])
        return dest

    @on('master')
    def spark(self):
        """Diagnose spark."""
        return super().spark()

    @on('master')
    def perf(self):
        """Diagnose performance for a Spark job between CPU and GPU."""
        return super().perf()

    @Diagnostic.banner
    def spark_job(self):
        """Diagnose spark via Dataproc job interface."""
        cpu_job = {
            'type': self.cluster.JOB_TYPE_PYSPARK,
            'file': super().get_diag_scripts('hello_world.py'),
            'properties': {
                'spark.rapids.sql.enabled': 'false',
            },
        }
        output = self.cluster.submit_job(cpu_job)
        self.check_spark_output(output, 'CPU')

        gpu_job = {
            'type': self.cluster.JOB_TYPE_PYSPARK,
            'file': super().get_diag_scripts('hello_world.py'),
            'properties': {
                'spark.rapids.sql.enabled': 'true',
                'spark.task.resource.gpu.amount': '0.5',
                'spark.rapids.sql.explain': 'ALL',
            },
        }
        output = self.cluster.submit_job(gpu_job)
        self.check_spark_output(output, 'GPU')

    @Diagnostic.banner
    def perf_job(self):
        """Diagnose performance for a Spark job via Dataproc job interface."""
        cpu_job = {
            'type': self.cluster.JOB_TYPE_PYSPARK,
            'file': super().get_diag_scripts('perf.py'),
            'properties': {
                'spark.rapids.sql.enabled': 'false',
            },
        }
        output = self.cluster.submit_job(cpu_job)
        cpu_time = self.check_perf_output(output)
        logger.info(f'CPU execution time: {cpu_time}')

        gpu_job = {
            'type': self.cluster.JOB_TYPE_PYSPARK,
            'file': super().get_diag_scripts('perf.py'),
            'properties': {
                'spark.rapids.sql.enabled': 'true',
                'spark.task.resource.gpu.amount': '0.5',
                'spark.rapids.sql.explain': 'ALL',
            },
        }
        output = self.cluster.submit_job(gpu_job)
        gpu_time = self.check_perf_output(output)
        logger.info(f'GPU execution time: {gpu_time}')

        self.evaluate_perf_result(cpu_time, gpu_time)


def main():
    """Main function."""
    fire.Fire(DiagDataproc)


if __name__ == '__main__':
    main()
