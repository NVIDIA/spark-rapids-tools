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

import argparse
from pyspark import SparkContext
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, min, max, avg, stddev, countDistinct, when, asc, round
import fnmatch
from pyspark.sql.types import *

def validation(spark, args):

    if not valid_input(spark,args):
        print('|--Please Check The Inputs --|')
        return

    result = top_level_metadata(spark, args.format, args.t1, args.t2, args.t1p, args.t2p, args.f)
    print('|--Top Level Metadata Info--|')
    print(result.show())

    result = metrics_metadata(spark, args.format, args.t1, args.t2, args.t1p, args.t2p, args.pk, args.i, args.e, args.f, args.p)
    if result.count() == 0:
        print(f'|--Table {args.t1} and Table {args.t2} has identical metadata info--|')
        print(result.show())
    else:
        print('|--Metadata Diff Info--|')
        print(result.show())

    print('|--Run Metadata Validation Success--|')

def valid_input(spark, args):
    """
    Check the input is valida for matadata validation tool
    1- valid table
    2- valid included column
    3- check format supported
    """
    if not valid_table(spark, args):
        return False
    if not valid_metadata_included_column(spark, args):
        return False
    if args.format != 'hive':
        print('|--Currently only support hive format--|')
    return True

def valid_table(spark, args):
    """
    Check if the tables exist
    """
    if not spark._jsparkSession.catalog().tableExists(args.t1):
        print(f'|--Table {args.t1} does not exist!--|')
        return False
    if not spark._jsparkSession.catalog().tableExists(args.t2):
        print(f'|--Table {args.t2} does not exist!--|')
        return False
    return True

def valid_metadata_included_column(spark, args):
    """
    Check if the included column valid
    """
    if args.i in ['None', 'all']:
        return True
    table_DF = load_table(spark, args.format, args.t1, args.t1p, args.pk, args.e, args.i, args.f, "")
    excluded_columns_list = [e.strip() for e in args.e.split(",")]
    verify_column = [i.strip() for i in args.i.split(",") if i not in excluded_columns_list]
    verify_DF = table_DF.select(verify_column)

    for c in verify_DF.schema.fields:
        # here only excluded 'date' because it will raise exception, we also should excluded str/map/nested
        if(any(fnmatch.fnmatch(c.dataType.simpleString(), pattern) for pattern in
                            ['*date*'])):
            print(f'|--Unsupported metadata included data type: {c.dataType.simpleString()} for column: {c}--|')
            return False
    return True

def top_level_metadata(spark, format, t1, t2, t1p, t2p, f):
    """
    Check whether the columns number and row count could match for table1 and table2
    """
    if format in ['parquet', 'orc', 'csv']:
        print('todo')
    elif format == "hive":
        results = []
        table_confs = [(t1,t1p), (t2, t2p)]

        for (table_name,partition) in table_confs:
            sql = f'select * from {table_name}'
            if any(cond != 'None' for cond in [partition, f]):
                where_clause = ' where ' + ' and '.join(x for x in [partition, f] if x != 'None')
                sql += where_clause
            df = spark.sql(sql)
            row_count = df.count()
            col_count = len(df.columns)
            results.append((table_name, row_count, col_count))
        resultsDF = spark.createDataFrame(results, ["TableName", "RowCount", "ColumnCount"])
        return resultsDF

def generate_metric_df(spark, table_DF, i, e, t1):
    """
    Return the metrics dataframe for table, the return dataframe should be like:
    +-----------+------------+------------+------------+-------------------+-------------------+
    |Column Name|   mintable1|   maxtable1|   avgtable1|       stddevtable1|countDistincttable1|
    +-----------+------------+------------+------------+-------------------+-------------------+
    |       col1|         1.0|        11.0|         6.0|    3.3166247903554|               11.0|
    |       col2|1.23456789E8|9.87654321E8|5.94837261E8|4.513124419597775E8|                2.0|
    |       ...
    |       coln|1.23456789E8|9.87654321E8|5.94837261E8|4.513124419597775E8|                2.0|
    +-----------+------------+------------+------------+-------------------+-------------------+
    """
    @F.udf(returnType=StringType())
    def map_to_string(data):
        # Sort the keys and values in the map
        sorted_data = sorted(data.items(), key=lambda x: x[0]) if isinstance(data, dict) else sorted(
            [(k, sorted(v)) for k, v in data.items()], key=lambda x: x[0])
        return str(dict(sorted_data))

    result = None
    agg_functions = [min, max, avg, stddev, countDistinct]
    # if not specified any included_columns, then get all numeric cols and string and map cols
    excluded_columns_list = [e.strip() for e in e.split(",")]
    metrics_cols = [i.strip() for i in i.split(",") if i not in excluded_columns_list]
    if i in ['None', 'all']:
        metrics_cols = [c.name for c in table_DF.schema.fields if
                        any(fnmatch.fnmatch(c.dataType.simpleString(), pattern) for pattern in ['*int*', '*decimal*', '*float*', '*double*', 'string', '*map*'])]
    map_metrics_cols = [c.name for c in table_DF.schema.fields if
                    any(fnmatch.fnmatch(c.dataType.simpleString(), pattern) for pattern in ['*map*'])]
    normal_metrics_cols = list(set(metrics_cols) - set(map_metrics_cols))
    for col in normal_metrics_cols:
        dfc = spark.createDataFrame(([col],), ["ColumnName"])
        table1_agg = table_DF.select(
            [f(col).alias(f.__name__ + t1) for f in
             agg_functions])
        tmp_df = dfc.join(table1_agg)
        if result is None:
            result = tmp_df
        else:
            result = result.union(tmp_df)

    for col in map_metrics_cols:
        dfc = spark.createDataFrame(([col],), ["ColumnName"])
        table1_agg = table_DF.select(
            [f(map_to_string(col)).alias(f.__name__ + t1) for f in
             agg_functions])
        tmp_df = dfc.join(table1_agg)
        if result is None:
            result = tmp_df
        else:
            result = result.union(tmp_df)
    return result

def metrics_metadata(spark, format, t1, t2, t1p, t2p, pk, i, e, f, p):
    """
    The different metadata of each column in each table(min/max/avg/stddev/count_distinct):
    (If the values are identical, then a specific cell is empty, aka NULL. So we only show differences),
    the result dataframe should be:
    +----------+------+------+------+------+-----------------+-----------------+-------------------+-------------------+-----------+-----------+
    |ColumnName| min_A| min_B| max_A| max_B|            avg_A|            avg_B|           stddev_A|           stddev_B|countdist_A|countdist_B|
    +----------+------+------+------+------+-----------------+-----------------+-------------------+-------------------+-----------+-----------+
    |      col1|      |      |12.000|11.000|6.090909090909091|              6.0|  3.477198454346414|    3.3166247903554|           |           |
    |      col3|      |      |      |      |5.090909090909091|5.181818181818182| 3.1766191290283903|  3.060005941764879|           |           |
    |      col8|12.340|12.330|      |      |       12.4345455|       12.4336364|0.30031195901474267|0.30064173786328213|          3|          4|
    +----------+------+------+------+------+-----------------+-----------------+-------------------+-------------------+-----------+-----------+
    """
    table1_DF = load_table(spark, format, t1, t1p, pk, e, i, f, "")
    table2_DF = load_table(spark, format, t2, t2p, pk, e, i, f, "")

    table_metric_df1 = generate_metric_df(spark, table1_DF, i, e, t1)
    table_metric_df2 = generate_metric_df(spark, table2_DF, i, e, t2)
    joined_table = table_metric_df1.alias("t1").join(table_metric_df2.alias("t2"), ["ColumnName"])

    cond = (round("t1.min" + t1, p) != round("t2.min" + t2, p)) | \
           (round("t1.max" + t1, p) != round("t2.max" + t2, p)) | \
           (round("t1.avg" + t1, p) != round("t2.avg" + t2, p)) | \
           (round("t1.stddev" + t1, p) != round("t2.stddev" + t2, p)) | \
           (round("t1.countDistinct" + t1, p) != round("t2.countDistinct" + t2, p))

    # apply condition on the joined table, return the final dataframe
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
    """
    Load dataframe according to different format type
    """
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
        if any(cond != 'None' for cond in [t1p,f]):
            where_clause = ' where ' + ' and '.join(x for x in [t1p, f] if x != 'None')
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