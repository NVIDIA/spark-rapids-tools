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
import logging
import os

import fire

from spark_rapids_dataproc_tools.csp import new_csp
from spark_rapids_dataproc_tools.data_validation import Validation

logger = logging.getLogger('data_validation_dataproc')

class DataValidationDataproc(Validation):
    """DataValidation tool for Dataproc."""

    #        validate = Validation(cluster, region, check, format, t1, t1p, t2, t2p, pk, e, i, f, o, of, p, debug)

    def __init__(self, cluster_name, region, check, format, t1, t1p, t2, t2p, pk, e, i, f, o, of, p, debug=False):
        super().__init__(debug)

        self.cluster = new_csp('dataproc', args={'cluster': cluster_name, 'region': region})
        self.format = format
        self.t1 = t1
        self.t2 = t2

    def on(node):  # pylint: disable=invalid-name,no-self-argument,too-many-function-args
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
        self.valid_metadata()
        self.valid_data()

    @on('master')  # pylint: disable=too-many-function-args
    def valid_metadata(self):
        print("-----------run valid metadata-------------")

    # @on('master')  # pylint: disable=too-many-function-args
    # def valid_data(self):
    #     if self.format == 'parquet':
    #         super().compare(self.t1, self.t2)
    #     print("-----------run valid data-------------------")

    @Validation.banner
    def valid_data(self):
        """Diagnose spark via Dataproc job interface."""
        print("-----------run valid data-------------------")

        print(self.t1)
        print(self.t2)
        compare_job = {
            'type': self.cluster.JOB_TYPE_PYSPARK,
            'file': super().get_validation_scripts('compare.py'),
            'properties': {
                'spark.rapids.sql.enabled': 'false',
            },
            'parameters': {
                't1': self.t1,
                't2': self.t2,
            },
        }
        output = self.cluster.submit_job(compare_job)
        print(output)
        # self.check_spark_output(output, 'CPU')



def main():
    """Main function."""
    fire.Fire(DataValidationDataproc)


if __name__ == '__main__':
    main()
