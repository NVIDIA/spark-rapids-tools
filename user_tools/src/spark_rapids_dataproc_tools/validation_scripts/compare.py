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
"""Performance test scripts for Spark job between CPU and GPU."""
import argparse
from pyspark import SparkContext        # pylint: disable=import-error
from pyspark.sql import SparkSession    # pylint: disable=import-error
from pyspark.sql.functions import col   # pylint: disable=import-error
import time

def compare(spark, t1, t2):
    print("---------yyyyyy",t1)
    print("---------yyyyyy", t2)
    table1 = spark.read.parquet(
        "gs://rapids-test/yuanli-tools-eventlog-temp/data-validation/",t1).createOrReplaceTempView("datavalid1")
    table2 = spark.read.parquet(
        "gs://rapids-test/yuanli-tools-eventlog-temp/data-validation/",t2).createOrReplaceTempView("datavalid2")

    # sql1 = "select d1.col1, d1.col3, d2.col3, d1.col4, d2.col4 \
    #         from datavalid1 d1 FULL OUTER JOIN datavalid2 d2 \
    #         on d1.col1=d2.col1 \
    #         where d1.col3 <> d2.col3 or d1.col4 <> d2.col4"
    sql1 = "select col1, col2 from datavalid1 \
                except \
                select col1, col2 from datavalid2"

    result1 = spark.sql(sql1)

    start_time = time.time()
    print('------------run validation success-----', result1.show())
    result1.show()
    print('-----yua---')
    print(result1.show())
    print(f'----------------Execution time: {time.time() - start_time}')

if __name__ == '__main__':


    parser = parser = argparse.ArgumentParser()
    parser.add_argument('t1',
                        help='table1')
    parser.add_argument('t2',
                        help='table2')
    args = parser.parse_args()

    sc = SparkContext(appName='validation')
    spark = SparkSession(sc)

    compare(spark, args.t1, args.t2)


