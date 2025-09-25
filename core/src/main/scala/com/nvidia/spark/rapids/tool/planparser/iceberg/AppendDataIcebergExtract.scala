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
package com.nvidia.spark.rapids.tool.planparser.iceberg

import com.nvidia.spark.rapids.tool.planparser.{InsertCmdExtractorTrait, InsertIntoHadoopExtract}

import org.apache.spark.sql.rapids.tool.store.WriteOperationMetadataTrait
import org.apache.spark.sql.rapids.tool.util.StringUtils

/**
 * Extracts metadata from an Iceberg AppendData operation.
 * This is subject to change depending on collecting more samples that show that sample text is
 * different.
 * A sample planInfo extracted from Spark-3.5.6 looks like:
 * ```json
 * {
 * "sparkPlanInfo": {
 *   "nodeName": "AppendData",
 *   "simpleString":
 *     "AppendData org.apache.spark.sql.execution.datasources.v2.DataSourceV2Strategy$$Lambda$$2293/
 *     1470843699@46290193, IcebergWrite(table=local.db.my_iceberg_table, format=PARQUET)",
 *   "children": [..],
 *   "metadata": {},
 *   "metrics": [] }
 * }
 *
 * @param nodeDescr description of the node to parse.
 */
class AppendDataIcebergExtract(
    override val nodeDescr: String) extends InsertIntoHadoopExtract(nodeDescr) {

  // Determines if a component string contains catalog information.
  // It returns true if the string starts with "IcebergWrite(".
  override def catalogPredicate(compStr: String): Boolean = {
    compStr.startsWith("IcebergWrite(")
  }

  // Retrieves a property value from the catalog information.
  // If found, it returns the trimmed value from that match.
  private def getPropertyFromCatalog(key: String): Option[String] = {
    // The (?m) flag enables multiline mode so that ^ matches the beginning of a line.
    val regex = s"""$key=\\s*([^,)]*)""".r
    regex.findFirstMatchIn(catalogStr).map(_.group(1).trim)
  }

  override def extractFormat(components: Seq[String]): String = {
    // Try to extract the format from the catalog string first.
    getPropertyFromCatalog("format") match {
      case Some(format) =>
        val formatLower = format.toLowerCase
        if (!formatLower.startsWith("iceberg")) {
          s"Iceberg${formatLower.capitalize}"
        } else {
          formatLower.capitalize
        }
      case None =>
        // If not found, fall back to the parent class method.
        StringUtils.UNKNOWN_EXTRACT
    }
  }

  // the format index is not relevant here  since it is extracted from the catalog string.
  override var formatIndex: Int = 3

  override def extractCatalog(components: Seq[String]): (String, String) = {
    // Attempt to extract the database and table from the catalog property "table"
    if (hasCatalog) {
      getPropertyFromCatalog("table") match {
        case Some(databaseTable) =>
          // Split by dot and extract the last two elements as database and table
          val parts = databaseTable.split("\\.")
          if (parts.length >= 2) {
            val db = parts(parts.length - 2)
            val table = parts.last
            (db, table)
          } else {
            (StringUtils.UNKNOWN_EXTRACT, StringUtils.UNKNOWN_EXTRACT)
          }
        case None =>
          (StringUtils.UNKNOWN_EXTRACT, StringUtils.UNKNOWN_EXTRACT)
      }
    } else {
      (StringUtils.UNKNOWN_EXTRACT, StringUtils.UNKNOWN_EXTRACT)
    }
  }
}

/**
 * Companion object for AppendDataIcebergExtract.
 * It provides methods to build write operation metadata and
 * to check if a given operation name corresponds to an Iceberg append operation.
 */
object AppendDataIcebergExtract extends InsertCmdExtractorTrait {
  // Constructs the write operation metadata by instantiating AppendDataExtract
  // and invoking its build method.
  def buildWriteOp(nodeDescr: String): WriteOperationMetadataTrait = {
    new AppendDataIcebergExtract(nodeDescr).build()
  }

  // Determines if the given operation name corresponds to an Iceberg append operation.
  def accepts(opName: String): Boolean = {
    opName.equals(IcebergHelper.EXEC_APPEND_DATA)
  }
}
