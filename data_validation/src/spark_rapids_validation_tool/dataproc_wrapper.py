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
from spark_rapids_validation_tool.data_validation_dataproc import DataValidationDataproc

class DataprocWrapper(object):

    def validation(self,
                   cluster: str,
                   region: str,
                   check: str = 'valid_metadata',
                   format: str = None,
                   t1: str = None,
                   t1p: str = None,
                   t2: str = None,
                   t2p: str = None,
                   pk: str = None,
                   e: str = None,
                   i: str = 'all',
                   f: str = None,
                   o: str = None,
                   of: str = 'parquet',
                   p: int = 4,
                   debug: bool = False) -> None:
        """
        Run data validation tool on remote Dataproc cluster to compare whether two tables have same results, one scenario is it will be easier for
        users to determine whether the Spark job using RAPIDS Accelerator(aka GPU Spark job)
        returns the same result as the CPU Spark job. Here we assume two tables have same column names.

        :param cluster: Name of the Dataproc cluster.
        :param region: Region of Dataproc cluster (e.g. us-central1)
        :param check: Metadata validation or Data validation (e.g.  valid_metadata or valid_data. )

        :param format: The format of tables, currently only support hive format. If the format is parquet/orc/csv, the t1 and t2 should be an absolute path. Options are [hive, orc, parquet, csv](e.g. --format=hive or --format=parquet)
        :param t1: The first table name, if the format is parquet/orc/csv, this value should be an absolute path. (e.g. --t1=table1)
        :param t1p: The first table’s partition clause. (e.g. --t1p 'partition1=p1 and partition2=p2')
        :param t2: The second table name, if the format is parquet/orc/csv, this value should be an absolute path.. (e.g. --t2=table2)
        :param t2p: The second table’s partition clause. (e.g. --t2p 'partition1=p1 and partition2=p2')
        :param pk: The Primary key columns(comma separated), pk is required for data_validation. (e.g. --pk=pk1,pk2,pk3).
        :param e: Exclude column option. What columns do not need to be involved in the comparison, default is None. (e.g. --e=col4,col5,col6)
        :param i: Include column option. What columns need to be involved in the comparison, default is ALL. (e.g. --i=col1,col2,col3)
        :param f: Condition to filter rows. (e.g. --f “col1=value1 and col2=value2”)
        :param o: Output directory, the tool will generate a data file to a path. (e.g. --o=/data/output)
        :param of: Output format, default is parquet. (e.g. --of=parquet)
        :param p: Precision, if it is set to 4 digits, then 0.11113 == 0.11114 will return true for numeric columns. (e.g. -p=4)
        :param debug: True or False to enable verbosity

        """

        if not cluster or not region:
            raise Exception('Invalid cluster or region for Dataproc environment. '
                            'Please provide options "--cluster=<CLUSTER_NAME> --region=<REGION>" properly.')

        validate = DataValidationDataproc(cluster, region, check, format, t1, t1p, t2, t2p, pk, e, i, f, o, of, p, debug)

        if any(p is None for p in [cluster, region, t1, t2, format]):
            print('|--cluster/region/format/t1/t2 should not be none--|')
            return
        if format not in ['hive', 'orc', 'parquet', 'csv']:
            print('|--format should be one of hive/parquet/orc/csv--|')
            return
        if check == 'valid_data' and pk is None:
            print('|--pk should be not be none if running valid_data--|')
            return
        getattr(validate, check)()

def main():
    fire.Fire(DataprocWrapper)

if __name__ == '__main__':
    main()