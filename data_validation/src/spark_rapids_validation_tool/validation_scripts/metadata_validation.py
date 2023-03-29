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

    result = top_level_metadata(spark, args.format, args.table1, args.table2, args.table1_partition, args.table2_partition, args.filter)
    print('|--Top Level Metadata Info--|')
    print(result.show())

    result = metrics_metadata(spark, args.format, args.table1, args.table2, args.table1_partition,
                              args.table2_partition, args.pk, args.include_column, args.exclude_column, args.filter, args.precision)
    if result.count() == 0:
        print(f'|--Table {args.table1} and Table {args.table2} has identical metadata info--|')
        print(result.show())
    else:
        print('|--Metadata Diff Info--|')
        print(result.show())

    save_result(result, args.output_path, args.output_format)
    print('|--Run Metadata Validation Success--|')

def save_result(df, path, output_format):
    df.write.mode("overwrite").format(output_format).save(path)

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
    if not spark._jsparkSession.catalog().tableExists(args.table1):
        print(f'|--Table {args.table1} does not exist!--|')
        return False
    if not spark._jsparkSession.catalog().tableExists(args.table2):
        print(f'|--Table {args.table2} does not exist!--|')
        return False
    return True

def valid_metadata_included_column(spark, args):
    """
    Check if the included column valid
    """
    if args.include_column in ['None', 'all']:
        return True
    table_DF = load_table(spark, args.format, args.table1, args.table1_partition, args.pk, args.exclude_column, args.include_column, args.filter, "")
    excluded_columns_list = [e.strip() for e in args.exclude_column.split(",")]
    verify_column = [i.strip() for i in args.include_column.split(",") if i not in excluded_columns_list]
    verify_DF = table_DF.select(verify_column)

    for c in verify_DF.schema.fields:
        # here only excluded 'date' because it will raise exception, we also should excluded str/map/nested
        if(any(fnmatch.fnmatch(c.dataType.simpleString(), pattern) for pattern in
                            ['*date*'])):
            print(f'|--Unsupported metadata included data type: {c.dataType.simpleString()} for column: {c}--|')
            return False
    return True

def top_level_metadata(spark, format, table1, table2, table1_partition, table2_partition, filter):
    """
    Check whether the columns number and row count could match for table1 and table2
    """
    if format in ['parquet', 'orc', 'csv']:
        print('todo')
    elif format == "hive":
        results = []
        table_confs = [(table1,table1_partition), (table2, table2_partition)]

        for (table_name,partition) in table_confs:
            sql = f'select * from {table_name}'
            if any(cond != 'None' for cond in [partition, filter]):
                where_clause = ' where ' + ' and '.join(x for x in [partition, filter] if x != 'None')
                sql += where_clause
            df = spark.sql(sql)
            row_count = df.count()
            col_count = len(df.columns)
            results.append((table_name, row_count, col_count))
        resultsDF = spark.createDataFrame(results, ["TableName", "RowCount", "ColumnCount"])
        return resultsDF

def generate_metric_df(spark, table_DF, include_column, exclude_column, table):
    """
    Return the metrics dataframe for table, the return dataframe should be like:
    +-----------+------------+------------+------------+-------------------+-------------------+
    |Column Name|   mintable1|   maxtable1|   avgtable1|       stddevtable1|countDistincttable1|
    +-----------+------------+------------+------------+-------------------+-------------------+
    |       col1|         1.0|        11.0|         6.0|                3.3|               11.0|
    |       col2|         1.1|         9.8|         5.9|                4.5|                2.0|
    |       ...
    |       coln|         1.3|         9.3|         5.0|                3.2|                6.0|
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
    excluded_columns_list = [e.strip() for e in exclude_column.split(",")]
    metrics_cols = [i.strip() for i in include_column.split(",") if i not in excluded_columns_list]
    if include_column in ['None', 'all']:
        metrics_cols = [c.name for c in table_DF.schema.fields if
                        any(fnmatch.fnmatch(c.dataType.simpleString(), pattern) for pattern in ['*int*', '*decimal*', '*float*', '*double*', 'string', '*map*'])]
    map_metrics_cols = [c.name for c in table_DF.schema.fields if
                    any(fnmatch.fnmatch(c.dataType.simpleString(), pattern) for pattern in ['*map*'])]
    normal_metrics_cols = list(set(metrics_cols) - set(map_metrics_cols))
    for col in normal_metrics_cols:
        dfc = spark.createDataFrame(([col],), ["ColumnName"])
        table1_agg = table_DF.select(
            [f(col).alias(f.__name__ + table) for f in
             agg_functions])
        tmp_df = dfc.join(table1_agg)
        if result is None:
            result = tmp_df
        else:
            result = result.union(tmp_df)

    for col in map_metrics_cols:
        dfc = spark.createDataFrame(([col],), ["ColumnName"])
        table1_agg = table_DF.select(
            [f(map_to_string(col)).alias(f.__name__ + table) for f in
             agg_functions])
        tmp_df = dfc.join(table1_agg)
        if result is None:
            result = tmp_df
        else:
            result = result.union(tmp_df)
    return result

def metrics_metadata(spark, format, table1, table2, table1_partition, table2_partition,
                     pk, include_column, exclude_column, filter, precision):
    """
    The different metadata of each column in each table(min/max/avg/stddev/count_distinct):
    (If the values are identical, then a specific cell is empty, aka NULL. So we only show differences),
    the result dataframe should be:
    |--Metadata Diff Info--|
    +----------+-----+-----+-----+-----+-----+-----+--------+--------+-----------+-----------+
    |ColumnName|min_A|min_B|max_A|max_B|avg_A|avg_B|stddev_A|stddev_B|countdist_A|countdist_B|
    +----------+-----+-----+-----+-----+-----+-----+--------+--------+-----------+-----------+
    |      col1|     |     | 12.0| 11.0| 6.09|  6.0|    3.48|    3.32|           |           |
    |      col3|     |     |     |     | 5.09| 5.18|    3.18|    3.06|           |           |
    |      col4|     |     |     |     |     |     |        |        |          4|          5|
    |      col6|     |     |     |     |     |     |        |        |         10|         11|
    |      col7|     |     |     |     |     |     |        |        |         10|         11|
    |      col8|12.34|12.33|     |     |     |     |        |        |          3|          4|
    +----------+-----+-----+-----+-----+-----+-----+--------+--------+-----------+-----------+
    """
    table1_DF = load_table(spark, format, table1, table1_partition, pk, exclude_column, include_column, filter, "")
    table2_DF = load_table(spark, format, table2, table2_partition, pk, exclude_column, include_column, filter, "")

    table_metric_df1 = generate_metric_df(spark, table1_DF, include_column, exclude_column, table1)
    table_metric_df2 = generate_metric_df(spark, table2_DF, include_column, exclude_column, table2)
    joined_table = table_metric_df1.alias("t1").join(table_metric_df2.alias("t2"), ["ColumnName"])

    cond = (round("t1.min" + table1, precision) != round("t2.min" + table2, precision)) | \
           (round("t1.max" + table1, precision) != round("t2.max" + table2, precision)) | \
           (round("t1.avg" + table1, precision) != round("t2.avg" + table2, precision)) | \
           (round("t1.stddev" + table1, precision) != round("t2.stddev" + table2, precision)) | \
           (round("t1.countDistinct" + table1, precision) != round("t2.countDistinct" + table2, precision))

    # apply condition on the joined table, return the final dataframe
    result_table = joined_table.select("ColumnName",
                                       when(round(col("t1.min"+table1), precision) != round(col("t2.min"+table2), precision), round(col("t1.min"+table1), precision)).otherwise('').alias("min_A"),
                                       when(round(col("t1.min"+table1), precision) != round(col("t2.min"+table2), precision), round(col("t2.min"+table2), precision)).otherwise('').alias("min_B"),
                                       when(round(col("t1.max"+table1), precision) != round(col("t2.max"+table2), precision), round(col("t1.max"+table1), precision)).otherwise('').alias("max_A"),
                                       when(round(col("t1.max"+table1), precision) != round(col("t2.max"+table2), precision), round(col("t2.max"+table2), precision)).otherwise('').alias("max_B"),
                                       when(round(col("t1.avg"+table1), precision) != round(col("t2.avg"+table2), precision), round(col("t1.avg"+table1), precision)).otherwise('').alias("avg_A"),
                                       when(round(col("t1.avg"+table1), precision) != round(col("t2.avg"+table2), precision), round(col("t2.avg"+table2), precision)).otherwise('').alias("avg_B"),
                                       when(round(col("t1.stddev"+table1), precision) != round(col("t2.stddev"+table2), precision), round(col("t1.stddev"+table1), precision)).otherwise('').alias("stddev_A"),
                                       when(round(col("t1.stddev"+table1), precision) != round(col("t2.stddev"+table2), precision), round(col("t2.stddev"+table2), precision)).otherwise('').alias("stddev_B"),
                                       when(round(col("t1.countDistinct"+table1), precision) != round(col("t2.countDistinct"+table2), precision), round(col("t1.countDistinct"+table1), precision)).otherwise('').alias("countdist_A"),
                                       when(round(col("t1.countDistinct"+table1), precision) != round(col("t2.countDistinct"+table2), precision), round(col("t2.countDistinct"+table2), precision)).otherwise('').alias("countdist_B")
                                       ).where(cond).sort(asc("ColumnName"))
    return result_table

def load_table(spark, format, table, table_partition, pk, exclude_column, include_column, filter, view_name):
    """
    Load dataframe according to different format type
    """
    if format in ['parquet', 'orc', 'csv']:
        # select column clause
        cols = '*' if i is None else include_column
        # cols = cols if e is None else cols + f", EXCEPT ({e}) " only works on databricks
        sql = f"select {pk},{cols} from {view_name}"
        # where clause
        where_clause = ""
        path = table
        if table_partition != 'None' and filter != 'None':
            where_clause = f" where {table_partition} and {filter}"
        elif table_partition != 'None':
            where_clause = f" where {table_partition}"
            # partition clause should be in real order as data path
            # path += partition_to_path(t1p)
        elif filter != 'None':
            where_clause = f" where {filter}"
        spark.read.format(format).load(path).createOrReplaceTempView(view_name)
        sql += where_clause
        result = spark.sql(sql)
        return result

    elif format == "hive":
        cols = '*' if include_column is None or include_column == 'all' else include_column
        sql = f"select {cols} from {table}"
        # where clause
        if any(cond != 'None' for cond in [table_partition,filter]):
            where_clause = ' where ' + ' and '.join(x for x in [table_partition, filter] if x != 'None')
            sql += where_clause

        df = spark.sql(sql)
        return df


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--format',
                        type=str,
                        help='The format of tables')
    parser.add_argument('--table1',
                        type=str,
                        help='table1')
    parser.add_argument('--table2',
                        type=str,
                        help='table2')
    parser.add_argument('--table1_partition',
                        type=str,
                        help='table1 partition')
    parser.add_argument('--table2_partition',
                        type=str,
                        help='table2 partition')
    parser.add_argument('--pk',
                        type=str,
                        help='primary key')
    parser.add_argument('--exclude_column',
                        type=str,
                        help='Exclude column option')
    parser.add_argument('--include_column',
                        type=str,
                        help='Include column option')
    parser.add_argument('--filter',
                        type=str,
                        help='Condition to filter rows')
    parser.add_argument('--output_path',
                        type=str,
                        help='Output directory')
    parser.add_argument('--output_format',
                        type=str,
                        help='Output format, default is parquet')
    parser.add_argument('--precision',
                        type=int,
                        help='Precision, default is 4')
    args = parser.parse_args()

    sc = SparkContext(appName='metadata-validation')
    spark = SparkSession(sc)

    validation(spark, args)