/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids.tool.planparser

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.execution.ui
import org.apache.spark.sql.execution.ui.SparkPlanGraphNode
import org.apache.spark.sql.rapids.tool.store.WriteOperationMetadataTrait
import org.apache.spark.sql.rapids.tool.util.StringUtils


class WriteOperationParserSuite extends AnyFunSuite {

  /**
   * Helper method to test `getWriteOpMetaFromNode`.
   *
   * @param node The input `SparkPlanGraphNode`.
   * @param expectedExecName The expected execution name.
   * @param expectedDataFormat The expected data format.
   * @param expectedOutputPath The expected output path (optional).
   * @param expectedOutputColumns The expected output columns (optional).
   * @param expectedWriteMode The expected write mode.
   * @param expectedTableName The expected table name.
   * @param expectedDatabaseName The expected database name.
   */
  private def testGetWriteOpMetaFromNode(
    node: SparkPlanGraphNode,
    expectedExecName: String,
    expectedDataFormat: String,
    expectedOutputPath: String,
    expectedOutputColumns: String,
    expectedWriteMode: String,
    expectedTableName: String,
    expectedDatabaseName: String,
    expectedPartitionCols: String): Unit = {

    val metadata: WriteOperationMetadataTrait =
      DataWritingCommandExecParser.getWriteOpMetaFromNode(node)

    assert(metadata.execName() == expectedExecName, "execName")
    assert(metadata.dataFormat() == expectedDataFormat, "dataFormat")
    assert(metadata.outputPath() == expectedOutputPath, "outputPath")
    assert(metadata.outputColumns() == expectedOutputColumns, "outputColumns")
    assert(metadata.writeMode() == expectedWriteMode, "writeMode")
    assert(metadata.table() == expectedTableName, "tableName")
    assert(metadata.dataBase() == expectedDatabaseName, "databaseName")
    assert(metadata.partitions() == expectedPartitionCols, "partitionCols")
  }

  // scalastyle:off line.size.limit
  test("getWriteOpMetaFromNode  — Unknown command") {
    // unknown cmd should return unknown values
    val node = new SparkPlanGraphNode(
      id = 2,
      name = "UnknownWrite",
      desc = "Some random description",
      Seq.empty
    )
    testGetWriteOpMetaFromNode(
      node,
      expectedExecName = StringUtils.UNKNOWN_EXTRACT,
      expectedDataFormat = StringUtils.UNKNOWN_EXTRACT,
      expectedOutputPath = StringUtils.UNKNOWN_EXTRACT,
      expectedOutputColumns = StringUtils.UNKNOWN_EXTRACT,
      expectedWriteMode = StringUtils.UNKNOWN_EXTRACT,
      expectedTableName = StringUtils.UNKNOWN_EXTRACT,
      expectedDatabaseName = StringUtils.UNKNOWN_EXTRACT,
      expectedPartitionCols = StringUtils.UNKNOWN_EXTRACT
    )
  }

  test("InsertIntoHadoopFsRelationCommand — No CatalogMeta") {
    // the writeOp writes into a table, without "catalogTable" information
    val node = new SparkPlanGraphNode(
      id = 1,
      name = "Execute InsertIntoHadoopFsRelationCommand",
      desc = "Execute InsertIntoHadoopFsRelationCommand gs://path/to/database/table1, " +
        "false, Parquet, " +
        "[serialization.format=1, mergeschema=false, __hive_compatible_bucketed_table_insertion__=true], " +
        "Append, `spark_catalog`.`database`.`table1`, org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe, " +
        "org.apache.spark.sql.execution.datasources.InMemoryFileIndex(gs://path/to/database/table1), " +
        "[col01, col02, col03]",
      Seq.empty
    )
    testGetWriteOpMetaFromNode(
      node,
      expectedExecName = "InsertIntoHadoopFsRelationCommand",
      expectedDataFormat = "Parquet",
      expectedOutputPath = "gs://path/to/database/table1",
      expectedOutputColumns = "col01;col02;col03",
      expectedWriteMode = "Append",
      expectedTableName = "table1",
      expectedDatabaseName = "database",
      expectedPartitionCols = StringUtils.UNKNOWN_EXTRACT
    )
  }

  test("InsertIntoHadoopFsRelationCommand — Format is 4th element") {
    // verify that the format is extracted correctly when the format is the 4th element in the
    // arguments list.
    val node = new SparkPlanGraphNode(
      id = 5,
      name = "Execute InsertIntoHadoopFsRelationCommand",
      desc = "Execute InsertIntoHadoopFsRelationCommand gs://path/to/database/table1, " +
        "false, [paths=(path)], Parquet, " +
        "[serialization.format=1, mergeschema=false, __hive_compatible_bucketed_table_insertion__=true], " +
        "Append, `spark_catalog`.`database`.`table1`, org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe, " +
        "org.apache.spark.sql.execution.datasources.InMemoryFileIndex(gs://path/to/database/table1), " +
        "[col01]",
      Seq.empty
    )
    testGetWriteOpMetaFromNode(
      node,
      expectedExecName = "InsertIntoHadoopFsRelationCommand",
      expectedDataFormat = "Parquet",
      expectedOutputPath = "gs://path/to/database/table1",
      expectedOutputColumns = "col01",
      expectedWriteMode = "Append",
      expectedTableName = "table1",
      expectedDatabaseName = "database",
      expectedPartitionCols = "paths=(path)"
    )
  }

  test("InsertIntoHadoopFsRelationCommand — Empty output columns") {
    // empty output columns appear as empty string.
    val node = new SparkPlanGraphNode(
      id = 5,
      name = "Execute InsertIntoHadoopFsRelationCommand",
      desc = "Execute InsertIntoHadoopFsRelationCommand gs://path/to/database/table1, " +
        "false, Parquet, " +
        "[serialization.format=1, mergeschema=false, __hive_compatible_bucketed_table_insertion__=true], " +
        "Append, `database`.`table1`, org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe, " +
        "org.apache.spark.sql.execution.datasources.InMemoryFileIndex(gs://path/to/database/table1), " +
        "[]",
      Seq.empty
    )
    testGetWriteOpMetaFromNode(
      node,
      expectedExecName = "InsertIntoHadoopFsRelationCommand",
      expectedDataFormat = "Parquet",
      expectedOutputPath = "gs://path/to/database/table1",
      expectedOutputColumns = "",
      expectedWriteMode = "Append",
      expectedTableName = "table1",
      expectedDatabaseName = "database",
      expectedPartitionCols = StringUtils.UNKNOWN_EXTRACT
    )
  }

  test("InsertIntoHadoopFsRelationCommand — Long schema") {
    // Long schema shows up as ellipses in the description.
    val node = new ui.SparkPlanGraphNode(
      id = 5,
      name = "Execute InsertIntoHadoopFsRelationCommand",
      desc = "Execute InsertIntoHadoopFsRelationCommand gs://path/to/database/table1, " +
        "false, [paths=(path)], Parquet, " +
        "[serialization.format=1, mergeschema=false, __hive_compatible_bucketed_table_insertion__=true], " +
        "Append, `spark_catalog`.`database`.`table1`, org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe, " +
        "org.apache.spark.sql.execution.datasources.InMemoryFileIndex(gs://path/to/database/table1), " +
        "[col01, col02, col03, ... 4 more fields]",
      Seq.empty
    )
    testGetWriteOpMetaFromNode(
      node,
      expectedExecName = "InsertIntoHadoopFsRelationCommand",
      expectedDataFormat = "Parquet",
      expectedOutputPath = "gs://path/to/database/table1",
      expectedOutputColumns = "col01;col02;col03;... 4 more fields",
      expectedWriteMode = "Append",
      expectedTableName = "table1",
      expectedDatabaseName = "database",
      expectedPartitionCols = "paths=(path)"
    )
  }

  test("InsertIntoHadoopFsRelationCommand — CatalogEntry and Partitions") {
    // Long schema shows up as ellipses in the description.
    val node = new ui.SparkPlanGraphNode(
      id = 5,
      name = "Execute InsertIntoHadoopFsRelationCommand",
      desc = "Execute InsertIntoHadoopFsRelationCommand gs://path/to/metastore/database1.db/tableName, " +
        "false, [pcol_00#298], Parquet, [serialization.format=1, mergeSchema=false, " +
        "__hive_compatible_bucketed_table_insertion__=true, \npartitionOverwriteMode=DYNAMIC], " +
        "Overwrite, CatalogTable(\n" +
        "Database: database1\nTable: tableName\nOwner: spark\n" +
        "Created Time: Wed Feb 19 00:07:14 UTC 2025\nLast Access: UNKNOWN\nCreated By: Spark 3.5.1\n" +
        "Type: MANAGED\nProvider: hive\nTable Properties: [transient_lastDdlTime=1739923634]\n" +
        "Location: gs://path/to/metastore/database1.db/tableName\n" +
        "Serde Library: org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe\n" +
        "InputFormat: org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat\n" +
        "OutputFormat: org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat\n" +
        "Storage Properties: [serialization.format=1]\nPartition Provider: Catalog\n" +
        "Partition Columns: [`pcol_00`]\nSchema: root\n |-- col_00: string (nullable = true)\n" +
        " |-- col_01: string (nullable = true)\n |-- col_02: integer (nullable = true)\n" +
        " |-- col_03: decimal(18,2) (nullable = true)\n |-- col_04: string (nullable = true)\n" +
        " |-- col_05: string (nullable = true)\n |-- col_06: integer (nullable = true)\n" +
        " |-- col_07: decimal(18,2) (nullable = true)\n |-- col_08: string (nullable = true)\n" +
        " |-- col_09: string (nullable = true)\n |-- col_10: integer (nullable = true)\n" +
        " |-- col_11: decimal(18,2) (nullable = true)\n |-- col_12: decimal(18,2) (nullable = true)\n" +
        " |-- col_13: long (nullable = true)\n |-- pcol_00: string (nullable = true)\n" +
        "), org.apache.spark.sql.execution.datasources.CatalogFileIndex@1eaf6b4e, " +
        "[col_00, col_01, col_02, col_03, col_04, col_05, col_06, col_07, col_08, col_09, col_10, " +
        "col_11, col_12, col_13, pcol_00]",
      Seq.empty
    )
    testGetWriteOpMetaFromNode(
      node,
      expectedExecName = "InsertIntoHadoopFsRelationCommand",
      expectedDataFormat = "HiveParquet",
      expectedOutputPath = "gs://path/to/metastore/database1.db/tableName",
      expectedOutputColumns = "col_00;col_01;col_02;col_03;col_04;col_05;col_06;col_07;col_08;col_09;col_10;col_11;col_12;col_13;pcol_00",
      expectedWriteMode = "Overwrite",
      expectedTableName = "tableName",
      expectedDatabaseName = "database1",
      expectedPartitionCols = "pcol_00"
    )
  }

  test("GpuInsertIntoHadoopFsRelationCommand — Broken RAPIDS formats") {
    // The GPU eventlog may have incorrect formats
    // https://github.com/NVIDIA/spark-rapids-tools/issues/1561
    // this test ensures that the tools can tolerate the broken formats by converting them to
    // proper formats.
    val testFileFormats = Seq(
      ("com.nvidia.spark.rapids.GpuParquetFileFormat@9f5022c", "Parquet"),
      ("com.nvidia.spark.rapids.GpuOrcFileFormat@123abc", "Orc"),
      ("com.nvidia.spark.rapids.GpuHiveTextFileFormat@123abc", "HiveText"),
      ("com.nvidia.spark.rapids.GpuHiveParquetFileFormat@123abc", "HiveParquet"),
      ("com.nvidia.spark.rapids.GpuDeltaFileFormat@123abc", "Delta")
    )
    testFileFormats.foreach { case (format, expectedDataFormat) =>
      val node = new SparkPlanGraphNode(
        id = 1,
        name = "Execute GpuInsertIntoHadoopFsRelationCommand",
        desc = s"Execute GpuInsertIntoHadoopFsRelationCommand gs://path/to/database/table1, " +
          s"false, $format, " +
          "[serialization.format=1, mergeschema=false, __hive_compatible_bucketed_table_insertion__=true], " +
          "Append, `spark_catalog`.`database`.`table1`, org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe, " +
          "org.apache.spark.sql.execution.datasources.InMemoryFileIndex(gs://path/to/database/table1), " +
          "[col01, col02, col03]",
        Seq.empty
      )
      testGetWriteOpMetaFromNode(
        node,
        expectedExecName = "GpuInsertIntoHadoopFsRelationCommand",
        expectedDataFormat = expectedDataFormat,
        expectedOutputPath = "gs://path/to/database/table1",
        expectedOutputColumns = "col01;col02;col03",
        expectedWriteMode = "Append",
        expectedTableName = "table1",
        expectedDatabaseName = "database",
        expectedPartitionCols = StringUtils.UNKNOWN_EXTRACT
      )
    }
  }

  test("GpuInsertIntoHadoopFsRelationCommand — Gpu log with CatalogTable entry") {
    // This test ensures that we can parse GpuInsert when there is a CatalogTable defined.
    // The format in this test should be HiveParquet because we extract the format from the serDe
    // library.
    val node = new ui.SparkPlanGraphNode(
      id = 5,
      name = "Execute GpuInsertIntoHadoopFsRelationCommand",
      desc = "Execute GpuInsertIntoHadoopFsRelationCommand gs://path/to/metastore/databaseName/table1, " +
        "false, [p0#23, p1#30], com.nvidia.spark.rapids.GpuParquetFileFormat@4be4680b, " +
        "[serialization.format=1, mergeSchema=false, __hive_compatible_bucketed_table_insertion__=true, " +
        "partitionOverwriteMode=DYNAMIC], " +
        "Overwrite, " +
        "CatalogTable(\nDatabase: databaseName\nTable: table1\nOwner: root\n" +
        "Created Time: Tue Feb 25 16:58:00 UTC 2025\\nLast Access: UNKNOWN\nCreated By: Spark 3.3.2\n" +
        "Type: EXTERNAL\nProvider: hive\nTable Properties: [transient_lastDdlTime=1740502680]\n" +
        "Location: gs://path/to/metastore/databaseName/table1\n" +
        "Serde Library: org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe\n" +
        "InputFormat: org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat\n" +
        "OutputFormat: org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat\n" +
        "Storage Properties: [serialization.format=1]\\nPartition Provider: Catalog\n" +
        "Partition Columns: [`p0`, `p1`]\n" +
        "Schema: root\n |-- col_00: string (nullable = true)\\n" +
        " |-- col_01: string (nullable = true)\n |-- col_02: string (nullable = true)\n" +
        " |-- col_03: double (nullable = true)\n |-- col_04: integer (nullable = true)\n" +
        " |-- col_05: double (nullable = true)\n |-- col_06: decimal (2, 4)\n" +
        " |-- p0: string (nullable = true)\n" +
        " |-- p1: string (nullable = true)\n), " +
        "org.apache.spark.sql.execution.datasources.CatalogFileIndex@8fc69d8f, " +
        "[col_00, col_01, col_02, col_03, col_04, col_05, col_06, p0, p1], false, 0",
      Seq.empty
    )

    testGetWriteOpMetaFromNode(
      node,
      expectedExecName = "GpuInsertIntoHadoopFsRelationCommand",
      expectedDataFormat = "HiveParquet",
      expectedOutputPath = "gs://path/to/metastore/databaseName/table1",
      expectedOutputColumns = "col_00;col_01;col_02;col_03;col_04;col_05;col_06;p0;p1",
      expectedWriteMode = "Overwrite",
      expectedTableName = "table1",
      expectedDatabaseName = "databaseName",
      expectedPartitionCols = "p0;p1"
    )
  }

  test("GpuInsertIntoHadoopFsRelationCommand — Insert into file (no Database/Table)") {
    // This tests when an exec does not have a table. Instead, it writes immediately to a file.
    // In that case, the write operation should not have a database/table names.
    val node = new ui.SparkPlanGraphNode(
      id = 5,
      name = "Execute GpuInsertIntoHadoopFsRelationCommand",
      desc = "Execute GpuInsertIntoHadoopFsRelationCommand " +
        "file:/tmp/local/file/random-uuid, false, " +
        "com.nvidia.spark.rapids.GpuParquetFileFormat@421e46ed, " +
        "[path=/tmp/local/file/random-uuid/], " +
        "Overwrite, [age, name], false, 0",
      Seq.empty
    )

    testGetWriteOpMetaFromNode(
      node,
      expectedExecName = "GpuInsertIntoHadoopFsRelationCommand",
      expectedDataFormat = "Parquet",
      expectedOutputPath = "file:/tmp/local/file/random-uuid",
      expectedOutputColumns = "age;name",
      expectedWriteMode = "Overwrite",
      expectedTableName = StringUtils.INAPPLICABLE_EXTRACT,
      expectedDatabaseName = StringUtils.INAPPLICABLE_EXTRACT,
      expectedPartitionCols = StringUtils.UNKNOWN_EXTRACT
    )
  }

  test("GpuInsertIntoHadoopFsRelationCommand — CatalogTable entry with path enclosed in brackets") {
    // Tests GPU eventlog that has defined a path between brackets
    val node = new ui.SparkPlanGraphNode(
      id = 5,
      name = "Execute GpuInsertIntoHadoopFsRelationCommand",
      desc = "Execute GpuInsertIntoHadoopFsRelationCommand " +
        "file:/home/work/spark_test/data/temporary_test.db/table1, " +
        "false, com.nvidia.spark.rapids.GpuParquetFileFormat@406479dc, " +
        "[path=file:/home/work/spark_test/data/temporary_test.db/table1], " +
        "Overwrite, CatalogTable(\n" +
        "Database: temporary_test\nTable: table1\nOwner: turing\n" +
        "Created Time: Fri Sep 01 16:31:17 UTC 2023\\nLast Access: UNKNOWN\n" +
        "Created By: Spark 3.3.1\nType: MANAGED\nProvider: parquet\n" +
        "Location: file:/home/work/spark_test/data/temporary_test.db/table1\n" +
        "Serde Library: org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe\n" +
        "InputFormat: org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat\n" +
        "OutputFormat: org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat\n" +
        "Schema: root\n" +
        " |-- _c0: integer (nullable = false)\n" +
        "), org.apache.spark.sql.execution.datasources.InMemoryFileIndex@f0ed3e64, [_c0], false, 0",
      Seq.empty
    )

    testGetWriteOpMetaFromNode(
      node,
      expectedExecName = "GpuInsertIntoHadoopFsRelationCommand",
      expectedDataFormat = "HiveParquet",
      expectedOutputPath = "file:/home/work/spark_test/data/temporary_test.db/table1",
      expectedOutputColumns = "_c0",
      expectedWriteMode = "Overwrite",
      expectedTableName = "table1",
      expectedDatabaseName = "temporary_test",
      expectedPartitionCols = StringUtils.UNKNOWN_EXTRACT
    )
  }

  test("AppendDataExecV1 — delta format") {
    val node = new SparkPlanGraphNode(
      id = 3,
      name = "AppendDataExecV1",
      // the description should include Delta keywords; otherwise it would be considered a spark Op.
      desc =
        s"""|AppendDataExecV1 [num_affected_rows#18560L, num_inserted_rows#18561L], DeltaTableV2(org.apache.spark.sql.SparkSession@5aa5327e,abfss://abfs_path,Some(CatalogTable(
            |Catalog: spark_catalog
            |Database: database
            |Table: tableName
            |Owner: root
            |Created Time: Wed Sep 15 16:47:47 UTC 2021
            |Last Access: UNKNOWN
            |Created By: Spark 3.1.1
            |Type: EXTERNAL
            |Provider: delta
            |Table Properties: [bucketing_version=2, delta.lastCommitTimestamp=1631724453000, delta.lastUpdateVersion=0, delta.minReaderVersion=1, delta.minWriterVersion=2]
            |Location: abfss://abfs_path
            |Serde Library: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe
            |InputFormat: org.apache.hadoop.mapred.SequenceFileInputFormat
            |OutputFormat: org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat
            |Partition Provider: Catalog
            |Schema: root
            | |-- field_00: string (nullable = true)
            | |-- field_01: string (nullable = true)
            | |-- field_02: string (nullable = true)
            | |-- field_03: string (nullable = true)
            | |-- field_04: string (nullable = true)
            | |-- field_05: string (nullable = true)
            | |-- field_06: string (nullable = true)
            | |-- field_07: string (nullable = true)
            | |-- field_08: string (nullable = true)
            |)),Some(spark_catalog.adl.tableName),None,Map()), Project [from_unixtime(unix_timestamp(current_timestamp(), yyyy-MM-dd HH:mm:ss, Some(Etc/UTC), false), yyyy-MM-dd HH:mm:ss, Some(Etc/UTC)) AS field_00#15200, 20240112 AS field_01#15201, load_func00 AS field_02#15202, completed AS field_03#15203, from_unixtime(unix_timestamp(current_timestamp(), yyyy-MM-dd HH:mm:ss, Some(Etc/UTC), false), yyyy-MM-dd HH:mm:ss, Some(Etc/UTC)) AS field_04#15204, from_unixtime(unix_timestamp(current_timestamp(), yyyy-MM-dd HH:mm:ss, Some(Etc/UTC), false), yyyy-MM-dd HH:mm:ss, Some(Etc/UTC)) AS field_05#15205, ddsdmsp AS field_06#15206, rename_01 AS field_07#15207, from_unixtime(unix_timestamp(current_timestamp(), yyyy-MM-dd HH:mm:ss, Some(Etc/UTC), false), yyyyMMdd, Some(Etc/UTC)) AS field_08#15208], org.apache.spark.sql.execution.datasources.v2.DataSourceV2Strategy$$$$Lambda$$12118/1719387317@5a111bfa, com.databricks.sql.transaction.tahoe.catalog.WriteIntoDeltaBuilder$$$$anon$$1@24257336
            |""".stripMargin,
      Seq.empty
    )
    testGetWriteOpMetaFromNode(
      node,
      expectedExecName = "AppendDataExecV1",
      expectedDataFormat = DeltaLakeHelper.getWriteFormat, // Special handling for DeltaLake
      expectedOutputPath = StringUtils.UNKNOWN_EXTRACT,
      expectedOutputColumns = StringUtils.UNKNOWN_EXTRACT,
      expectedWriteMode = StringUtils.UNKNOWN_EXTRACT,
      expectedTableName = StringUtils.UNKNOWN_EXTRACT,
      expectedDatabaseName = StringUtils.UNKNOWN_EXTRACT,
      expectedPartitionCols = StringUtils.UNKNOWN_EXTRACT
    )
  }

  test("InsertIntoHiveTable cmd — No catalog prefix") {
    // The catalog piece has `database`.`table`
    val node = new ui.SparkPlanGraphNode(
      id = 5,
      name = "Execute InsertIntoHiveTable",
      desc = "Execute InsertIntoHiveTable `database1`.`table1`, " +
        "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, [dt=Some(2025-01-01)], true, false, " +
        "[col1, col2, col3]",
      Seq.empty
    )
    testGetWriteOpMetaFromNode(
      node,
      expectedExecName = "InsertIntoHiveTable",
      expectedDataFormat = "HiveText",
      expectedOutputPath = StringUtils.INAPPLICABLE_EXTRACT,
      expectedOutputColumns = "col1;col2;col3",
      expectedWriteMode = "Overwrite",
      expectedTableName = "table1",
      expectedDatabaseName = "database1",
      expectedPartitionCols = "dt=Some(2025-01-01)"
    )
  }

  test("InsertIntoHiveTable cmd — Catalog prefix") {
    // The catalog piece has `spark_catalog`.`database`.`table`
    val node = new ui.SparkPlanGraphNode(
      id = 5,
      name = "Execute InsertIntoHiveTable",
      desc = "Execute InsertIntoHiveTable `spark_catalog`.`database1`.`table1`, " +
        "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, [dt=Some(2025-01-01)], false, false, " +
        "[col1, col2, col3]",
      Seq.empty
    )
    testGetWriteOpMetaFromNode(
      node,
      expectedExecName = "InsertIntoHiveTable",
      expectedDataFormat = "HiveText",
      expectedOutputPath = StringUtils.INAPPLICABLE_EXTRACT,
      expectedOutputColumns = "col1;col2;col3",
      expectedWriteMode = "Append",
      expectedTableName = "table1",
      expectedDatabaseName = "database1",
      expectedPartitionCols = "dt=Some(2025-01-01)"
    )
  }

  test("InsertIntoHiveTable cmd — Dynamic partitions") {
    // The partitions argument is missing
    val node = new ui.SparkPlanGraphNode(
      id = 5,
      name = "Execute InsertIntoHiveTable",
      desc = "Execute InsertIntoHiveTable `spark_catalog`.`database1`.`table1`, " +
        "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, false, True, " +
        "[col1, col2, col3]",
      Seq.empty
    )
    testGetWriteOpMetaFromNode(
      node,
      expectedExecName = "InsertIntoHiveTable",
      expectedDataFormat = "HiveText",
      expectedOutputPath = StringUtils.INAPPLICABLE_EXTRACT,
      expectedOutputColumns = "col1;col2;col3",
      expectedWriteMode = "Append",
      expectedTableName = "table1",
      expectedDatabaseName = "database1",
      expectedPartitionCols = StringUtils.UNKNOWN_EXTRACT
    )
  }

  test("GpuInsertIntoHiveTable — Gpu logs profiler case") {
    // The GPU eventlog has 2 different format arguments. For now, we pick the serDe library.
    val node = new ui.SparkPlanGraphNode(
      id = 5,
      name = "Execute GpuInsertIntoHiveTable",
      desc = "Execute GpuInsertIntoHiveTable `database1`.`table1`, " +
        "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe, " +
        "[date=Some(20240621)], " +
        "org.apache.spark.sql.hive.rapids.GpuHiveParquetFileFormat@3dcbd2c4, true, false, " +
        "[col_00, col_01, col_02, col_03, col_04, col_05, col_06, col_07, col_08, col_09, col_10, " +
        "col_11, col_12, col_13, col_14, col_15, col_16, col_17, col_18, col_19, col_20, col_21, " +
        "col_22, col_23, ... 122 more fields]",
      Seq.empty
    )

    testGetWriteOpMetaFromNode(
      node,
      expectedExecName = "GpuInsertIntoHiveTable",
      expectedDataFormat = "HiveParquet",
      expectedOutputPath = StringUtils.INAPPLICABLE_EXTRACT,
      expectedOutputColumns = "col_00;col_01;col_02;col_03;col_04;col_05;col_06;col_07;col_08;col_09;" +
        "col_10;col_11;col_12;col_13;col_14;col_15;col_16;col_17;col_18;col_19;col_20;col_21;" +
        "col_22;col_23;... 122 more fields",
      expectedWriteMode = "Overwrite",
      expectedTableName = "table1",
      expectedDatabaseName = "database1",
      expectedPartitionCols = "date=Some(20240621)"
    )
  }
  // scalastyle:on line.size.limit
}
