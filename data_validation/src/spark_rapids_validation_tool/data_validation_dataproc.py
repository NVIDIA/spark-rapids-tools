# Copyright (c) 2023, NVIDIA CORPORATION.
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

import fire
from spark_rapids_validation_tool.csp import new_csp
from spark_rapids_validation_tool.data_validation import Validation

class DataValidationDataproc(Validation):
    """DataValidation tool for Dataproc."""
    def __init__(self, cluster_name, region, check, format, table1, table1_partition, table2,
                 table2_partition, pk, excluded_column: str, included_column: str, filter: str,
                 output_dir, output_format, precision, debug=False):
        super().__init__(debug)

        self.cluster = new_csp('dataproc', args={'cluster': cluster_name, 'region': region})
        self.format = format
        self.table1 = table1
        self.table2 = table2
        self.table1_partition = table1_partition
        self.table2_partition = table2_partition
        self.pk = pk
        self.excluded_column = excluded_column
        self.included_column = included_column
        self.filter = filter
        self.output_dir = output_dir
        self.output_format = output_format
        self.precision = precision

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

    def format_conf_with_quotation(self,conf):
        if conf is None:
            return 'None'
        else:
            return conf.replace('\'', '\\\'')

    @on('master')  # pylint: disable=too-many-function-args
    def valid_metadata(self):
        """metadata validation spark via Dataproc job interface."""
        print("|--Start Running Metadata Validation.....--|")
        if self.excluded_column is None:
            excluded_column = 'None'
        else:
            excluded_column = self.convert_tuple_to_string(self.excluded_column)

        compare_job = {
            'type': self.cluster.JOB_TYPE_PYSPARK,
            'file': super().get_validation_scripts('metadata_validation.py'),
            'properties': {
                'spark.rapids.sql.enabled': 'false',
            },
            'parameters': [
                f'--t1={self.table1}',
                f'--t2={self.table2}',
                f'--format={self.format}',
                f'--t1p={self.table1_partition}',
                f'--t2p={self.table2_partition}',
                f'--i={self.convert_tuple_to_string(self.included_column)}',
                f'--pk={self.pk}',
                f'--e={excluded_column}',
                f'--f={self.format_conf_with_quotation(self.filter)}',
                f'--o={self.output_dir}',
                f'--of={self.output_format}',
                f'--p={self.precision}'
            ]
        }
        output = self.cluster.submit_job(compare_job)
        print(output)

    def convert_tuple_to_string(self, conf):
        '''fire automatically convert config with comma from str to tuple'''
        if isinstance(conf, tuple):
            return ','.join(map(str, conf))
        elif isinstance(conf, str):
            return conf
        else:
            raise Exception(f'invalid type of conf : {conf}')
        fi

    @Validation.banner
    def valid_data(self):
        """data validation spark via Dataproc job interface."""
        print("|--Start Running Data Validation.....--|")
        compare_job = {
            'type': self.cluster.JOB_TYPE_PYSPARK,
            'file': super().get_validation_scripts('dataset_validation.py'),
            'properties': {
                'spark.rapids.sql.enabled': 'false',
            },
            'parameters':[
                f'--t1={self.table1}',
                f'--t2={self.table2}',
                f'--format={self.format}',
                f'--t1p={self.table1_partition}',
                f'--t2p={self.table2_partition}',
                f'--i={self.convert_tuple_to_string(self.included_column)}',
                f'--pk={self.pk}',
                f'--e={self.excluded_column}',
                f'--f={self.format_conf_with_quotation(self.filter)}',
                f'--o={self.output_dir}',
                f'--of={self.output_format}',
                f'--p={self.precision}'
            ]
        }

        output = self.cluster.submit_job(compare_job)
        print(output)

def main():
    """Main function."""
    fire.Fire(DataValidationDataproc)

if __name__ == '__main__':
    main()