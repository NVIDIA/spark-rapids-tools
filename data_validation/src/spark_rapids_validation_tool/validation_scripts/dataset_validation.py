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
from pyspark import SparkContext        # pylint: disable=import-error
from pyspark.sql import SparkSession, DataFrame, functions as F    # pylint: disable=import-error
from pyspark.sql.functions import col, when   # pylint: disable=import-error
import time
from pyspark.sql.types import *
import fnmatch
from functools import reduce

def validation(spark, args):

    if not valid_input(spark,args):
        print('|--Please Check The Inputs --|')
        return

    # valid PK(s) only in table1
    result = valid_pk_only_in_one_table(spark, args.format, args.table1, args.table2, args.table1_partition, args.table2_partition, args.pk,
                                        args.exclude_column, args.include_column, args.filter, args.output_path, args.output_format)
    print(f'|--PK(s) only in {args.table1} :--|')
    print(result.show())
    # valid PK(s) only in table2
    result = valid_pk_only_in_one_table(spark, args.format, args.table2, args.table1, args.table2_partition, args.table1_partition, args.pk,
                                        args.exclude_column, args.include_column, args.filter, args.output_path, args.output_format)
    print(f'|--PK(s) only in {args.table2} :--|')
    print(result.show())

    # valid result table with the same PK but different values for that column(s)
    result = get_cols_diff_with_same_pk(spark, args.format, args.table1, args.table2, args.pk, args.table1_partition, args.table2_partition,
                                        args.filter, args.include_column, args.exclude_column, args.precision)
    print("|--Columns with same PK(s) but diff values :--|")
    print(result.show())
    print('|--------------run validation success-------|')

    save_result(result, args.output_path, args.output_format)

def save_result(df, path, output_format):
    if path != 'None':
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
    table_DF = load_table(spark, args.format, args.table1, args.table1_partition, args.pk, args.include_column, args.filter, "")
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

# def row_counts(spark, format, table, t1p, t1f):
#     """Get the row counts of a table according"""
#     sql = "select count(*) from table"
#     where_clause = ""
#     if t1p != 'None' and t1f !='None':
#         where_clause = f" where {t1p} and {t1f}"
#     elif t1p != 'None':
#         where_clause = f" where {t1p}"
#     elif t1f != 'None':
#         where_clause = f" where {t1f}"
#     if format in ['parquet', 'orc', 'csv']:
#         path = table
#         spark.read.format(format).load(path).createOrReplaceTempView("table")
#         sql += where_clause
#
#         result = spark.sql(sql)
#         return result
#     elif format == "hive":
#         print("----todo---hive--")
#         return 0

def valid_pk_only_in_one_table(spark, format, table1, table2, table1_partition, table2_partition, pk,
                               exclude_column, include_column, filter, output_path, output_format):
    """valid PK(s) only in one table"""
    if format in ['parquet', 'orc', 'csv']:

        # load table1
        load_table(spark, format, table1, table1_partition, pk, include_column, filter, "table1")
        # load table2
        load_table(spark, format, table2, table2_partition, pk, include_column, filter, "table2")

        sql = f"select {pk} from table1 except select {pk} from table2"
        result = spark.sql(sql)
        return result

    elif format == "hive":
        sql1 = f"select {pk} from {table1} "
        sql2 = f"select {pk} from {table2} "

        if any(cond != 'None' for cond in [table1_partition,filter]):
            where_clause = ' where ' + ' and '.join(x for x in [table1_partition, filter] if x != 'None')
            sql1 += where_clause
        if any(cond != 'None' for cond in [table2_partition,filter]):
            where_clause = ' where ' + ' and '.join(x for x in [table2_partition, filter] if x != 'None')
            sql2 += where_clause
        sql = sql1 + " except " + sql2
        result = spark.sql(sql)
        return result

    return

def get_cols_diff_with_same_pk(spark, format, table1_name, table2_name, pk, table1_partition, table2_partition, filter, included_columns, excluded_columns, precision):
    if format in ['parquet', 'orc', 'csv']:
        pk_list = [i.strip() for i in pk.split(",")]
        included_columns_list = [i.strip() for i in included_columns.split(",")]
        excluded_columns_list = [e.strip() for e in excluded_columns.split(",")]
        select_columns = [f't1.{p}' for p in pk.split(',')] + [f't1.{c} as t1_{c}, t2.{c} as t2_{c}' for c in included_columns_list if
                                                               c not in excluded_columns_list]
        sql = f"""
                    SELECT {', '.join(select_columns)}
                    FROM table1 t1
                    FULL OUTER JOIN table2 t2 ON {' AND '.join([f't1.{c} = t2.{c}' for c in pk_list])}
                    WHERE ({' or '.join([f't1.{c} <> t2.{c}' for c in included_columns_list if c not in excluded_columns_list])} )
                """
        if table1_partition != 'None':
            table1_partition = [p.strip() for p in table1_partition.split("and")]
            sql += ' AND ( ' + ' AND '.join([f't1.{p} ' for p in table1_partition]) + ' )'

        if filter != 'None':
            filters = [f.strip() for f in filter.split("and")]
            sql += ' AND ( ' + ' AND '.join([f't1.{f} ' for f in filters]) + ' )'

        # Execute the query and return the result
        result = spark.sql(sql)

        return result
    elif format == "hive":
        # todo: convert nested type to string using udf
        pk_list = [i.strip() for i in pk.split(",")]
        included_columns_list = [i.strip() for i in included_columns.split(",")]
        excluded_columns_list = [e.strip() for e in excluded_columns.split(",")]
        @F.udf(returnType=StringType())
        def map_to_string(data):
            # Sort the keys and values in the map
            sorted_data = sorted(data.items(), key=lambda x: x[0]) if isinstance(data, dict) else sorted(
                [(k, sorted(v)) for k, v in data.items()], key=lambda x: x[0])
            return str(dict(sorted_data))

        table_DF1 = load_table(spark, format, table1_name, table1_partition, pk, included_columns, filter, "table1")
        table_DF2 = load_table(spark, format, table2_name, table2_partition, pk, included_columns, filter, "table2")

        if included_columns == 'all':
            included_columns_list = list(set(table_DF1.columns) - set(excluded_columns_list) - set(pk_list))
        joined_table = table_DF1.alias("t1").join(table_DF2.alias("t2"), pk_list)

        map_cols = []
        cond = []
        for c in table_DF1.schema.fields:
            if (any(fnmatch.fnmatch(c.dataType.simpleString(), pattern) for pattern in
                    ['*map*'])):
                map_cols.append(c.name)
        normal_cols = list(set(table_DF1.columns) - set(map_cols))
        for c in normal_cols:
            cond.append(col("t1." + c) != col("t2." + c))
        for c in map_cols:
            cond.append(map_to_string(col("t1." + c)) != map_to_string(col("t2." + c)))

        normal_columns_list = [(when(col('t1.' + c) != col('t2.' + c), col('t1.' + c)).otherwise('').alias('t1_' + c),
                                when(col('t2.' + c) != col('t1.' + c), col('t2.' + c)).otherwise('').alias('t2_' + c)) for c in
                               normal_cols if
                               c not in excluded_columns_list and c not in pk_list]

        map_columns_list = [(when(map_to_string(col('t1.' + c)) != map_to_string(col('t2.' + c)), map_to_string(col('t1.' + c))).otherwise('').alias('t1_' + c),
                                when(map_to_string(col('t2.' + c)) != map_to_string(col('t1.' + c)), map_to_string(col('t2.' + c))).otherwise('').alias('t2_' + c))
                               for c in
                               map_cols if
                               c not in excluded_columns_list]
        select_columns_list = normal_columns_list + map_columns_list
        ##flatten select_columns_list
        select_columns_flattened_list = [select_column for sublist in select_columns_list for select_column in sublist]
        select_columns = [col('t1.' + p) for p in pk.split(',')] + select_columns_flattened_list

        result_table = joined_table.select(select_columns).where(reduce(lambda a, b: a | b,cond))

        return result_table

def load_table(spark, format, table, table_partition, pk, include_column, filter, view_name):
    if format in ['parquet', 'orc', 'csv']:
        # select column clause
        cols = '*' if include_column is None else include_column
        # cols = cols if e is None else cols + f", EXCEPT ({e}) "
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
        if include_column in ['None', 'all']:
            sql = f"select * from {table} "
        else:
            # select_column = [include_column.strip() for include_column in i.split(",") if
            #                  i not in excluded_columns_list]
            # select_column_str = select_column
            sql = f"select {pk},{include_column} from {table} "

        if any(cond != 'None' for cond in [table_partition, filter]):
            where_clause = ' where ' + ' and '.join(x for x in [table_partition, filter] if x != 'None')
            sql += where_clause

        result = spark.sql(sql)
        return result

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

    sc = SparkContext(appName='data-validation')
    spark = SparkSession(sc)

    validation(spark, args)