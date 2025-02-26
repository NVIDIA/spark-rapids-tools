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

import org.scalatest.FunSuite

import org.apache.spark.sql.execution.ui
import org.apache.spark.sql.execution.ui.SparkPlanGraphNode
import org.apache.spark.sql.rapids.tool.store.WriteOperationMetadataTrait
import org.apache.spark.sql.rapids.tool.util.StringUtils


class WriteOperationParserSuite extends FunSuite {

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
    expectedDatabaseName: String): Unit = {

    val metadata: WriteOperationMetadataTrait =
      DataWritingCommandExecParser.getWriteOpMetaFromNode(node)

    assert(metadata.execName() == expectedExecName, "execName")
    assert(metadata.dataFormat() == expectedDataFormat, "dataFormat")
    assert(metadata.outputPath() == expectedOutputPath, "outputPath")
    assert(metadata.outputColumns() == expectedOutputColumns, "outputColumns")
    assert(metadata.writeMode() == expectedWriteMode, "writeMode")
    assert(metadata.table() == expectedTableName, "tableName")
    assert(metadata.dataBase() == expectedDatabaseName, "databaseName")
  }
  // scalastyle:off line.size.limit
  test("InsertIntoHadoopFsRelationCommand - Common case") {
    val node = new SparkPlanGraphNode(
      id = 1,
      name = "Execute InsertIntoHadoopFsRelationCommand",
      desc = "Execute InsertIntoHadoopFsRelationCommand gs://path/to/database/table1, " +
        "false, Parquet, " +
        "[serialization.format=1, mergeschema=false, __hive_compatible_bucketed_table_insertion__=true], " +
        "Append, `spark_catalog`.`database`.`table`, org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe, " +
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
      expectedDatabaseName = "database"
    )
  }

  test("getWriteOpMetaFromNode - Unknown command") {
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
      expectedDatabaseName = StringUtils.UNKNOWN_EXTRACT
    )
  }

  test("getWriteOpMetaFromNode - Gpu logs profiler case") {
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
        name = "Execute InsertIntoHadoopFsRelationCommand",
        desc = s"Execute InsertIntoHadoopFsRelationCommand gs://path/to/database/table1, " +
          s"false, $format, " +
          "[serialization.format=1, mergeschema=false, __hive_compatible_bucketed_table_insertion__=true], " +
          "Append, `spark_catalog`.`database`.`table`, org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe, " +
          "org.apache.spark.sql.execution.datasources.InMemoryFileIndex(gs://path/to/database/table1), " +
          "[col01, col02, col03]",
        Seq.empty
      )
      testGetWriteOpMetaFromNode(
        node,
        expectedExecName = "InsertIntoHadoopFsRelationCommand",
        expectedDataFormat = expectedDataFormat,
        expectedOutputPath = "gs://path/to/database/table1",
        expectedOutputColumns = "col01;col02;col03",
        expectedWriteMode = "Append",
        expectedTableName = "table1",
        expectedDatabaseName = "database"
      )
    }
  }

  test("AppendDataExecV1 - delta format") {
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
      expectedDatabaseName = StringUtils.UNKNOWN_EXTRACT
    )
  }

  test("InsertIntoHadoopFsRelationCommand - Empty output columns") {
    val node = new SparkPlanGraphNode(
      id = 5,
      name = "Execute InsertIntoHadoopFsRelationCommand",
      desc = "Execute InsertIntoHadoopFsRelationCommand gs://path/to/database/table1, " +
        "false, Parquet, " +
        "[serialization.format=1, mergeschema=false, __hive_compatible_bucketed_table_insertion__=true], " +
        "Append, `spark_catalog`.`database`.`table`, org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe, " +
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
      expectedDatabaseName = "database"
    )
  }

  test("InsertIntoHadoopFsRelationCommand - Format is 4th element") {
    val node = new SparkPlanGraphNode(
      id = 5,
      name = "Execute InsertIntoHadoopFsRelationCommand",
      desc = "Execute InsertIntoHadoopFsRelationCommand gs://path/to/database/table1, " +
        "false, [paths=(path)], Parquet, " +
        "[serialization.format=1, mergeschema=false, __hive_compatible_bucketed_table_insertion__=true], " +
        "Append, `spark_catalog`.`database`.`table`, org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe, " +
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
      expectedDatabaseName = "database"
    )
  }

  test("InsertIntoHadoopFsRelationCommand - Long schema") {
    // Long schema will show up as ellipses in the description
    val node = new ui.SparkPlanGraphNode(
      id = 5,
      name = "Execute InsertIntoHadoopFsRelationCommand",
      desc = "Execute InsertIntoHadoopFsRelationCommand gs://path/to/database/table1, " +
        "false, [paths=(path)], Parquet, " +
        "[serialization.format=1, mergeschema=false, __hive_compatible_bucketed_table_insertion__=true], " +
        "Append, spark_catalog`.`database`.`table`, org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe, " +
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
      expectedDatabaseName = "database"
    )
  }
  // scalastyle:on line.size.limit
}
