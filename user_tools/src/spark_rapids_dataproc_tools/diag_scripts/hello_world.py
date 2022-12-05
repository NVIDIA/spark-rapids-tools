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

"""Hello-world Spark Application on CPU and GPU."""

if __name__ == '__main__':
    from pyspark import SparkContext        # pylint: disable=import-error
    from pyspark.sql import SparkSession    # pylint: disable=import-error
    from pyspark.sql.functions import col   # pylint: disable=import-error
    sc = SparkContext(appName='hello_world')
    spark = SparkSession(sc)
    df = spark.range(1, 10000).select(col('id'))
    df2 = spark.range(1, 10000).select(col('id'))
    print('------------run hello success-----', df.join(df2, df.id == df2.id).count())
