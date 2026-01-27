/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.planparser.iceberg

import com.nvidia.spark.rapids.tool.planparser.SupportedOpStub
import com.nvidia.spark.rapids.tool.planparser.ops.OpTypes
import com.nvidia.spark.rapids.tool.plugins.PropConditionOnSparkExtTrait

import org.apache.spark.sql.rapids.tool.util.EventUtils.SPARK_CATALOG_REGEX

/**
 * Helper object for Iceberg related utilities.
 * This includes methods to check if Iceberg is enabled in the Spark properties,
 * and to extract the catalog type among other static functionalities.
 */
object IcebergHelper extends PropConditionOnSparkExtTrait {
  // An Iceberg app is identified using the following properties from spark properties.
  override val extensionRegxMap: Map[String, String] = Map(
    "spark.sql.extensions" -> ".*IcebergSparkSessionExtensions.*"
  )

  /**
   * used to identify if a property key represents a Spark catalog configuration
   * (e.g., spark.sql.catalog.my_catalog), which is one way to detect that Iceberg is being used
   * in the application, since Iceberg requires catalog configuration in Spark properties.
   * @param properties spark properties captured from the eventlog environment details
   * @return true if any spark catalog is defined, false otherwise
   */
  private def isSparkCatalogDefined(properties: collection.Map[String, String]): Boolean = {
    properties.keys.exists(key => SPARK_CATALOG_REGEX.pattern.matcher(key).matches())
  }

  // For Iceberg, RAPIDS only supports running against the Hadoop filesystem catalog.
  private val SUPPORTED_CATALOGS = Set("hadoop")

  // Iceberg metadata table suffixes used to identify metadata table scans in BatchScan operations.
  // When querying Iceberg metadata tables through the catalog API, the table name appears with
  // a metadata table suffix in the BatchScan node description.
  //
  // Examples in node.desc:
  //   - "BatchScan local.db.table.snapshots[...]"
  //   - "BatchScan catalog.database.table.manifests[...]"
  //   - "BatchScan table.files[...]"
  //
  // Reference: https://iceberg.apache.org/docs/latest/spark-queries/#querying-with-sql
  //
  // Supported Iceberg metadata tables:
  //   - snapshots: Lists all snapshots in the table
  //   - manifests: Lists manifest files for current snapshot
  //   - files: Lists current data files (alias for data_files)
  //   - history: Shows table history and snapshots
  //   - partitions: Shows partition information
  //   - all_manifests: Lists all manifest files
  //   - all_data_files: Lists all data files in the table
  //
  // Note: These are NOT matched against file paths like "/metadata/snap-*.avro" because
  // Iceberg metadata tables accessed via DataSource V2 show table names, not file paths.
  val ICEBERG_METADATA_TABLE_SUFFIXES: Set[String] = Set(
    ".snapshots",
    ".manifests",
    ".files",
    ".history",
    ".partitions",
    ".all_manifests",
    ".all_data_files"
  )

  val EXEC_APPEND_DATA: String = "AppendData"
  // Note: Spark plan shows "MergeRows" (without Exec suffix).
  // SupportedOpStub.execID will auto-append "Exec" for CSV matching.
  val EXEC_MERGE_ROWS: String = "MergeRows"
  // ReplaceData is the write operator for copy-on-write MERGE INTO operations.
  val EXEC_REPLACE_DATA: String = "ReplaceData"
  // WriteDelta is the write operator for merge-on-read MERGE INTO operations.
  // It writes "delete files" to track changes instead of rewriting data files.
  val EXEC_WRITE_DELTA: String = "WriteDelta"

  // A Map between the spark node name and the SupportedOpStub.
  // Note that AppendDataExec is not supported for Iceberg.
  //
  // MERGE INTO operations use two different strategies:
  // - Copy-on-Write (CoW): MergeRows -> ReplaceData (rewrites data files)
  // - Merge-on-Read (MoR): MergeRows -> WriteDelta (writes delete files)
  val DEFINED_EXECS: Map[String, SupportedOpStub] = Map(
    EXEC_APPEND_DATA ->
      SupportedOpStub(
        EXEC_APPEND_DATA,
        // The writeOp is not supported in Iceberg
        isSupported = false,
        opType = Option(OpTypes.WriteExec)
      ),
    EXEC_MERGE_ROWS ->
      SupportedOpStub(
        EXEC_MERGE_ROWS,
        // MergeRows is used in Iceberg MERGE INTO operations.
        isSupported = false,
        opType = Option(OpTypes.Exec)
      ),
    EXEC_REPLACE_DATA ->
      SupportedOpStub(
        EXEC_REPLACE_DATA,
        // ReplaceData is the write operator for copy-on-write MERGE INTO.
        isSupported = false,
        opType = Option(OpTypes.WriteExec)
      ),
    EXEC_WRITE_DELTA ->
      SupportedOpStub(
        EXEC_WRITE_DELTA,
        // WriteDelta is the write operator for merge-on-read MERGE INTO.
        // Writes "delete files" instead of rewriting data files.
        isSupported = false,
        opType = Option(OpTypes.WriteExec)
      )
  )

  /**
   * Checks if the properties indicate that the application is using Iceberg.
   * This can be checked by looking for keywords in one of the keys defined in
   * extensionRegxMap or if any spark catalog is set.
   *
   * @param properties spark properties captured from the eventlog environment details
   * @return true if the properties indicate that it is an Iceberg app.
   */
  override def eval(properties: collection.Map[String, String]): Boolean = {
    super.eval(properties) || isSparkCatalogDefined(properties)
  }

  /**
   * Extracts the catalog type from the spark properties.
   * This is needed as some ops may not be supported depending on the catalog type.
   * @param properties spark properties captured from the eventlog environment details
   * @return the catalog type if found, None otherwise
   */
  def getCatalogType(properties: collection.Map[String, String]): Option[String] = {
    // find the spark catalog property and then get the property that has "catalog.type"
    properties.keys
      .find(key => SPARK_CATALOG_REGEX.pattern.matcher(key).matches())
      .flatMap { catalog =>
        properties.get(s"$catalog.type")
      }
  }

  /**
   * Checks if the catalog type is supported by RAPIDS.
   * If the catalog type is not found, it defaults to true.
   * @param properties spark properties captured from the eventlog environment details
   * @return true if the catalog type is supported or not found, false otherwise
   */
  def isSparkCatalogSupported(properties: collection.Map[String, String]): Boolean = {
    getCatalogType(properties) match {
      case Some(catalog) =>
        SUPPORTED_CATALOGS.contains(catalog.toLowerCase)
      case _ =>
        // we could not extract the catalog, default to True.
        true
    }
  }
}
