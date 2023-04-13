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

"""Wrapper class to run tools associated with RAPIDS Accelerator for Apache Spark plugin."""
import fire
import yaml
from spark_rapids_validation_tool.data_validation_dataproc import DataValidationDataproc

class DataprocWrapper(object):

    def validation_parse(self,
                   cluster,
                   region,
                   check,
                   format,
                   table1,
                   table1_partition,
                   table2,
                   table2_partition,
                   pk,
                   exclude_column,
                   include_column,
                   filter,
                   output_path,
                   output_format,
                   precision,
                   debug,
                   spark_conf):
        """
        Run data validation tool on remote Dataproc cluster to compare whether two tables have same results, one scenario is it will be easier for
        users to determine whether the Spark job using RAPIDS Accelerator(aka GPU Spark job)
        returns the same result as the CPU Spark job. Here we assume two tables have same column names.

        :param cluster: Name of the Dataproc cluster.
        :param region: Region of Dataproc cluster (e.g. us-central1)
        :param check: Metadata validation or Data validation (e.g.  valid_metadata or valid_data. )

        :param format: The format of tables, currently only support hive format. If the format is parquet/orc/csv, the t1 and t2 should be an absolute path. Options are [hive, orc, parquet, csv](e.g. --format=hive or --format=parquet)
        :param table1: The first table name, if the format is parquet/orc/csv, this value should be an absolute path. (e.g. --table1=table1)
        :param table1_partition: The first table’s partition clause. (e.g. --table1_partition=partition1='p1')
        :param table2: The second table name, if the format is parquet/orc/csv, this value should be an absolute path.. (e.g. --table2=table2)
        :param table2_partition: The second table’s partition clause. (e.g. --table2_partition=partition1='p1')
        :param pk: The Primary key columns(comma separated), pk is required for data_validation. (e.g. --pk=pk1,pk2,pk3).
        :param exclude_column: Exclude column option. What columns do not need to be involved in the comparison, default is None. (e.g. --exclude_column=col4,col5,col6)
        :param include_column: Include column option. What columns need to be involved in the comparison, default is ALL. (e.g. --include_column=col1,col2,col3)
        :param filter: Condition to filter rows. (e.g. --filter “col1=value1 and col2=value2”)
        :param output_path: Output directory, the tool will generate a data file to a path. (e.g. --output_path=/data/output)
        :param output_format: Output format, default is parquet. (e.g. --output_format=parquet)
        :param precision: Precision, if it is set to 4 digits, then 0.11113 == 0.11114 will return true for numeric columns. (e.g. --precision=4)
        :param debug: True or False to enable verbosity

        """

        if not cluster or not region:
            raise Exception('Invalid cluster or region for Dataproc environment. '
                            'Please provide options "--cluster=<CLUSTER_NAME> --region=<REGION>" properly.')

        validate = DataValidationDataproc(cluster, region, check, format, table1, table1_partition, table2,
                                          table2_partition, pk, exclude_column, include_column, filter,
                                          output_path, output_format, precision, debug, spark_conf)

        if any(p is None for p in [cluster, region, table1, table2, format]):
            print('|--cluster/region/format/table1/table2 should not be none--|')
            return
        if format not in ['hive', 'orc', 'parquet', 'csv']:
            print('|--format should be one of hive/parquet/orc/csv--|')
            return
        if check == 'valid_data' and pk is None:
            print('|--pk should be not be none if running valid_data--|')
            return
        getattr(validate, check)()

    def validation(self, conf_file: str):
        with open(conf_file, "r") as file:
            validate_conf = yaml.safe_load(file)
        spark_conf = validate_conf['sparkConf']
        tool_conf = validate_conf['toolConf']
        self.validation_parse(tool_conf['cluster'],
                         tool_conf['region'],
                         tool_conf['check'],
                         tool_conf['format'],
                         tool_conf['table1'],
                         tool_conf['table1_partition'],
                         tool_conf['table2'],
                         tool_conf['table2_partition'],
                         tool_conf['pk'],
                         tool_conf['exclude_column'],
                         tool_conf['include_column'],
                         tool_conf['filter'],
                         tool_conf['output_path'],
                         tool_conf['output_format'],
                         tool_conf['precision'],
                         tool_conf['debug'],
                         spark_conf)

def main():
    fire.Fire(DataprocWrapper)

if __name__ == '__main__':
    main()