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
from pyspark.sql.functions import col, min, max, avg, stddev, countDistinct, when
import time
import fnmatch
from pyspark.sql.types import DoubleType


def validation(spark, args):
    print("---------yyyyyy",args.t1)
    print("---------yyyyyy", args.t2)
    print("---------yyyyyy", args.t1p)
    print("---------top level metadata", args.f)
    print('\n')


    result = top_level_metadata(spark, args.format, args.t1, args.t2, args.t1p, args.t2p, args.f)
    print(result.show())

    # A result table with the same PK but different values for that column(s)
    result = metrics_metadata(spark, args.format, args.t1, args.t2, args.t1p, args.t2p, args.pk, args.i, args.e, args.f, args.p)
    print('--')


    start_time = time.time()
    print('------------run validation success-----')
    print(f'----------------Execution time: {time.time() - start_time}')

def top_level_metadata(spark, format, t1, t2, t1p, t2p, f):
    if format in ['parquet', 'orc', 'csv']:
        print('todo')
    elif format == "hive":
        results = []
        table_names = [t1, t2]
        where_clause = ''
        if t1p != 'None' and f != 'None':
            where_clause = f" where {t1p} and {f}"
        elif t1p != 'None':
            where_clause = f" where {t1p}"
        elif f != 'None':
            where_clause = f" where {f}"

        print(f'------where clause-------{where_clause}------')
        for table_name in table_names:
            sql = f'select * from {table_name}'
            sql += where_clause
            print('------top_level_metadata------')
            print(sql)
            df = spark.sql(sql)
            row_count = df.count()
            col_count = len(df.columns)
            results.append((table_name, row_count, col_count))
        resultsDF = spark.createDataFrame(results, ["TableName", "RowCount", "ColumnCount"])
        return resultsDF

def generate_metric_df(spark, table_DF, i, t1):
    result = None
    agg_functions = [min, max, avg, stddev, countDistinct]
    # if not specified any included_columns, then get all numeric cols
    metrics_cols = i
    if i in ['None', 'all']:
        metrics_cols = [c.name for c in table_DF.schema.fields if
                        any(fnmatch.fnmatch(c.dataType.simpleString(), pattern) for pattern in ['*int*', '*decimal*'])]
    print('-----metrics_cols---')
    print(metrics_cols)
    for col in metrics_cols:
        dfc = spark.createDataFrame(([col],), ["ColumnName"])
        table1_agg = table_DF.select(
            [f(col).cast(DoubleType()).alias(f.__name__ + t1) for f in
             agg_functions])
        tmp_df = dfc.join(table1_agg)
        if result is None:
            result = tmp_df
        else:
            result = result.union(tmp_df)
    return result

def metrics_metadata(spark, format, t1, t2, t1p, t2p, pk, i, e, f, p):
    print("---")
    # todo: set precision
    # spark, format, t1, t1p, pk, e, i, f, view_name
    # table1_DF = load_table(spark, format, t1, t1p, t2p, i, f)
    table1_DF = load_table(spark, format, t1, t1p, pk, e, i, f, "")
    table2_DF = load_table(spark, format, t2, t2p, pk, e, i, f, "")

    table_metric_df1 = generate_metric_df(spark, table1_DF, i, t1)
    print('----table_metric_df1------')
    print(table_metric_df1.show())
    table_metric_df2 = generate_metric_df(spark, table2_DF, i, t2)
    print('----table_metric_df2------')
    print(table_metric_df2.show())
    # join both dataframes based on ColumnName   table1 and table2 should be the result df
    joined_table = table_metric_df1.join(table_metric_df2, ["ColumnName"])

    # define condition for selecting rows
    cond = (col("t1.min"+t1) != col("t2.min"+t2)) | \
           (col("t1.max"+t1) != col("t2.max"+t2)) | \
           (col("t1.avg"+t1) != col("t2.avg"+t2)) | \
           (col("t1.stddev"+t1) != col("t2.stddev"+t2)) | \
           (col("t1.countDistinct"+t1) != col("t2.countDistinct"+t2))

    print('----joined---table---')
    print(joined_table.show())
    # apply condition on the joined table
    # result_table = joined_table.select("ColumnName",
    #                                    when(cond, col("t1.min"+t1)).otherwise(None).alias("min_A"),
    #                                    when(cond, col("t2.min"+t2)).otherwise(None).alias("min_B"),
    #                                    when(cond, col("t1.max"+t1)).otherwise(None).alias("max_A"),
    #                                    when(cond, col("t2.max"+t2)).otherwise(None).alias("max_B"),
    #                                    when(cond, col("t1.avg"+t1)).otherwise(None).alias("avg_A"),
    #                                    when(cond, col("t2.avg"+t2)).otherwise(None).alias("avg_B"),
    #                                    when(cond, col("t1.stddev"+t1)).otherwise(None).alias("stddev_A"),
    #                                    when(cond, col("t2.stddev"+t2)).otherwise(None).alias("stddev_B"),
    #                                    when(cond, col("t1.countDistinct"+t1)).otherwise(None).alias("countdist_A"),
    #                                    when(cond, col("t2.countDistinct"+t2)).otherwise(None).alias("countdist_B")
    #                                    ).where(cond)
    #
    # return result_table

    """
    select ColumnName, 
if t1.mintable == t2.mintable then null else t1.mintable as 'min_A',
if t1.mintable == t2.mintable then null else t2.mintable as 'min_B',

if t1.maxtable == t2.maxtable then null else t1.maxtable as 'max_A',
if t1.maxtable == t2.maxtable then null else t2.maxtable as 'max_B',

if t1.avgtable == t2.avgtable then null else t1.avgtable as 'avg_A',
if t1.avgtable == t2.avgtable then null else t2.avgtable as 'avg_B',

if t1.stddevtable == t2.stddevtable then null else t1.stddevtable as 'stddev_A',
if t1.stddevtable == t2.stddevtable then null else t2.stddevtable as 'stddev_B',

if t1.countDistincttable == t2.countDistincttable then null else t1.countDistincttable as 'countdist_A',
if t1.countDistincttable == t2.countDistincttable then null else t2.countDistincttable as 'countdist_B',

from table1 t1 join table2 t2 on t1.ColumnName=t2.ColumnName
where t1.mintable <> t2.mintable or
      t1.maxtable <> t2.maxtable or
      t1.avgtable <> t2.avgtable or
      t1.stddevtable <> t2.stddevtable or
      t1.countDistincttable <> t2.countDistincttable
    """

def load_table(spark, format, t1, t1p, pk, e, i, f, view_name):
    if format in ['parquet', 'orc', 'csv']:
        # select column clause
        cols = '*' if i is None else i
        # cols = cols if e is None else cols + f", EXCEPT ({e}) "
        sql = f"select {pk},{cols} from {view_name}"
        # where clause
        where_clause = ""
        path = t1
        if t1p != 'None' and f != 'None':
            where_clause = f" where {t1p} and {f}"
        elif t1p != 'None':
            where_clause = f" where {t1p}"
            # partition clause should be in real order as data path
            # path += partition_to_path(t1p)
        elif f != 'None':
            where_clause = f" where {f}"

        print(f'--------load_table-sql--{sql}---')
        spark.read.format(format).load(path).createOrReplaceTempView(view_name)
        sql += where_clause
        result = spark.sql(sql)
        # result1 = spark.sql(sql1)

        # print(result)
        print(result)
    elif format == "hive":
        cols = '*' if i is None or i == 'all' else i
        sql = f"select {cols} from {t1}"
        # where clause
        where_clause = ""
        if t1p != 'None' and f != 'None':
            where_clause = f" where {t1p} and {f}"
        elif t1p != 'None':
            where_clause = f" where {t1p}"
            # partition clause should be in real order as data path
            # path += partition_to_path(t1p)
        elif f != 'None':
            where_clause = f" where {f}"
        sql += where_clause
        print('----------load table-metadata-----')
        print(sql)
        df = spark.sql(sql)
        return df




def partition_to_path(partition_str, path):
    partition = {}
    if partition_str:
        partition_items = partition_str.split("and")
        partition = dict(item.split("=") for item in partition_items)
    partition_path = "/".join([f"{col}={val}" for col, val in partition.items()])
    return f"{path}/{partition_path}".replace(" ", "")


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--format',
                        type=str,
                        help='The format of tables')
    parser.add_argument('--t1',
                        type=str,
                        help='table1')
    parser.add_argument('--t2',
                        type=str,
                        help='table2')
    parser.add_argument('--t1p',
                        type=str,
                        help='table1 partition')
    parser.add_argument('--t2p',
                        type=str,
                        help='table2 partition')
    parser.add_argument('--pk',
                        type=str,
                        help='primary key')
    parser.add_argument('--e',
                        type=str,
                        help='Exclude column option')
    parser.add_argument('--i',
                        type=str,
                        help='Include column option')
    parser.add_argument('--f',
                        type=str,
                        help='Condition to filter rows')
    parser.add_argument('--o',
                        type=str,
                        help='Output directory')
    parser.add_argument('--of',
                        type=str,
                        help='Output format, default is parquet')
    parser.add_argument('--p',
                        type=int,
                        help='Precision, default is 4')
    args = parser.parse_args()

    sc = SparkContext(appName='metadata-validation')
    spark = SparkSession(sc)
    print("aaaaaat1",args.t1)
    print("aaaaaat2", args.t2)
    print("iiiiii", args.i)
    print("fffff", args.f)
    print("eeeee", args.e)
    print("pkpkpk", args.pk)
    print("t1p", args.t1p)

    validation(spark, args)


