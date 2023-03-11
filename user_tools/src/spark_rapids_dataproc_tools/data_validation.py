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

    def __init__(self, debug=False):
        if debug:
            logger.setLevel(logging.DEBUG)

        # Diagnostic summary
        self.summary = {}


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