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
import argparse
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, avg, stddev, countDistinct, when, asc, round

import time
import fnmatch
from pyspark.sql.types import DoubleType

def validation(spark, args):

    result = top_level_metadata(spark, args.format, args.t1, args.t2, args.t1p, args.t2p, args.f)
    print('-------top level metadata info-------')
    print(result.show())

    # A result table with the same PK but different values for that column(s)
    result = metrics_metadata(spark, args.format, args.t1, args.t2, args.t1p, args.t2p, args.pk, args.i, args.e, args.f, args.p)
    print('-------metadata diff info-------')
    print(result.show())

    start_time = time.time()
    print('------------run validation success-----')

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
        for table_name in table_names:
            sql = f'select * from {table_name}'
            sql += where_clause
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
    # spark, format, t1, t1p, pk, e, i, f, view_name
    table1_DF = load_table(spark, format, t1, t1p, pk, e, i, f, "")
    table2_DF = load_table(spark, format, t2, t2p, pk, e, i, f, "")

    table_metric_df1 = generate_metric_df(spark, table1_DF, i, t1)
    table_metric_df2 = generate_metric_df(spark, table2_DF, i, t2)
    joined_table = table_metric_df1.alias("t1").join(table_metric_df2.alias("t2"), ["ColumnName"])

    # cond = (round(col("t1.min"+t1),p) != round(col("t2.min"+t2)),p) | \
    #        (round(col("t1.max"+t1),p) != round(col("t2.max"+t2)),p) | \
    #        (round(col("t1.avg"+t1),p) != round(col("t2.avg"+t2)),p) | \
    #        (round(col("t1.stddev"+t1),p) != round(col("t2.stddev"+t2)),p) | \
    #        (round(col("t1.countDistinct"+t1),p) != round(col("t2.countDistinct"+t2),p))

    cond = (round(col("t1.min" + t1),4) <> round(col("t2.min" + t2)),4) | \
           (col("t1.max" + t1) <> col("t2.max" + t2)) | \
           (col("t1.avg" + t1) <> col("t2.avg" + t2)) | \
           (col("t1.stddev" + t1) <> col("t2.stddev" + t2)) | \
           (col("t1.countDistinct" + t1) <> col("t2.countDistinct" + t2))

    # apply condition on the joined table
    result_table = joined_table.select("ColumnName",
                                       when(col("t1.min"+t1) != col("t2.min"+t2), col("t1.min"+t1)).otherwise('').alias("min_A"),
                                       when(col("t1.min"+t1) != col("t2.min"+t2), col("t2.min"+t2)).otherwise('').alias("min_B"),
                                       when(col("t1.max"+t1) != col("t2.max"+t2), col("t1.max"+t1)).otherwise('').alias("max_A"),
                                       when(col("t1.max"+t1) != col("t2.max"+t2), col("t2.max"+t2)).otherwise('').alias("max_B"),
                                       when(col("t1.avg"+t1) != col("t2.avg"+t2), col("t1.avg"+t1)).otherwise('').alias("avg_A"),
                                       when(col("t1.avg"+t1) != col("t2.avg"+t2), col("t2.avg"+t2)).otherwise('').alias("avg_B"),
                                       when(col("t1.stddev"+t1) != col("t2.stddev"+t2), col("t1.stddev"+t1)).otherwise('').alias("stddev_A"),
                                       when(col("t1.stddev"+t1) != col("t2.stddev"+t2), col("t2.stddev"+t2)).otherwise('').alias("stddev_B"),
                                       when(col("t1.countDistinct"+t1) != col("t2.countDistinct"+t2), col("t1.countDistinct"+t1)).otherwise('').alias("countdist_A"),
                                       when(col("t1.countDistinct"+t1) != col("t2.countDistinct"+t2), col("t2.countDistinct"+t2)).otherwise('').alias("countdist_B")
                                       ).where(cond).sort(asc("ColumnName"))
    return result_table

def load_table(spark, format, t1, t1p, pk, e, i, f, view_name):
    if format in ['parquet', 'orc', 'csv']:
        # select column clause
        cols = '*' if i is None else i
        # cols = cols if e is None else cols + f", EXCEPT ({e}) " only works on databricks
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
        spark.read.format(format).load(path).createOrReplaceTempView(view_name)
        sql += where_clause
        result = spark.sql(sql)
        return result

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
        df = spark.sql(sql)
        return df


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

    validation(spark, args)


