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
"""Data validation tool basic program."""

import json
import logging
import re
import sys
from typing import Callable
from pyspark.sql import SparkSession
import fire
import pkg_resources

# from spark_rapids_dataproc_tools.utilities import get_log_dict, run_cmd

# Setup logging
logger = logging.getLogger('validation')
#
# consoleHandler = logging.StreamHandler(sys.stdout)
# logFormatter = logging.Formatter('%(message)s')
# consoleHandler.setFormatter(logFormatter)
# logger.addHandler(consoleHandler)
#
logger.setLevel(logging.INFO)


class Validation:
    """Data Validation tool basic class."""
   #        validate = Validation(cluster, region, check, format, t1, t1p, t2, t2p, pk, e, i, f, o, of, p, debug)

    i: str
    e: str

    def __init__(self, debug=False):
        if debug:
            logger.setLevel(logging.DEBUG)

        # Diagnostic summary
        self.summary = {}

    def convert_tuple_to_string(self, conf):
        if isinstance(conf, tuple):
            return ','.join(map(str, conf))
        elif isinstance(conf, tuple):
            return conf
        else:
            raise Exception(f'invalid type of conf : {conf}')
        fi

    def banner(func: Callable):   # pylint: disable=no-self-argument
        """Banner decorator."""
        def wrapper(self, *args, **kwargs):
            name = func.__name__    # pylint: disable=no-member
            logger.info('*** Running validation function "%s" ***', name)

            result = True
            try:
                func(self, *args, **kwargs)     # pylint: disable=not-callable

            except Exception as exception:    # pylint: disable=broad-except
                logger.error('Error: %s', exception)
                result = False

            if result:
                logger.info('*** Check "%s": PASS ***', name)
            else:
                logger.info('*** Check "%s": FAIL ***', name)

            # Save result into summary
            if name in self.summary:
                self.summary[name] = any([result, self.summary[name]])
            else:
                self.summary[name] = result

        return wrapper

    def run_spark_submit(self, options, capture='all'):
        """Run spark application via spark-submit command."""
        cmd = ['$SPARK_HOME/bin/spark-submit']
        cmd += options
        stdout, stderr = self.run_cmd(cmd, capture=capture)
        return stdout + stderr

    def get_validation_scripts(self, name):
        """Get diagnostic script path by name"""
        return pkg_resources.resource_filename(__name__, 'validation_scripts/' + name)

    def compare(self, t1, t2):

        print('-'*40)

        def run(opts):
            output = self.run_spark_submit(opts + [self.get_validation_scripts('compare.py')])
            print(output)

        cpu_opts = ['--master', 'yarn']
        cpu_opts += ['--conf', 'spark.rapids.sql.enabled=false']

        cpu_time = run(cpu_opts)
        logger.info('CPU execution time: %s', cpu_time)

    # work
#     def add2(self):
#         print(self.cpu_table_path)
#         print(self.gpu_table_path)
#         print("-----add2---")
#
#     def compare(self, cpu_df, gpu_df):
#         #
#         print(cpu_df.count)
#         print(gpu_df.count)
#         print("--------compare------")
#
#     def compare_result(self):
#         # create spark session
#         spark = (SparkSession
#                  .builder
#                  .master("spark://yuanli-System-Product-Name:7077")
#                  .appName("data-validation-tool")
#                  .getOrCreate())
# #.appName(args.mainClass)
#
#         # load to dataframe /home/yuanli/work/project/data-validation/test-dataset/cpu_demo
#         df_cpu = spark.read.format("parquet").load(self.cpu_table_path)
#         df_gpu = spark.read.format("parquet").load(self.gpu_table_path)
#
#         self.compare(df_cpu, df_gpu)
#
#     def compare_result_gcs(self):
#         # create spark session
#         spark = (SparkSession
#                  .builder
#                  .master("yarn")
#                  .appName("data-validation-tool")
#                  .getOrCreate())
# #.appName(args.mainClass)
#
#         # load to dataframe /home/yuanli/work/project/data-validation/test-dataset/cpu_demo
#         df_cpu = spark.read.format("parquet").load(self.cpu_table_path)
#         df_gpu = spark.read.format("parquet").load(self.gpu_table_path)
#
#         self.compare(df_cpu, df_gpu)


def main():
    """Main function."""
    fire.Fire(Validation)


if __name__ == '__main__':
    main()