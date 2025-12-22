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

import com.nvidia.spark.rapids.tool.planparser.SupportedOpStub
import com.nvidia.spark.rapids.tool.planparser.ops.OpTypes

import org.apache.spark.sql.rapids.tool.util.EventUtils.SPARK_CATALOG_REGEX

/**
 * Helper object for Iceberg related utilities.
 * This includes methods to check if Iceberg is enabled in the Spark properties,
 * and to extract the catalog type among other static functionalities.
 */
object IcebergHelper {
  // An Iceberg app is identified using the following properties from spark properties.
  private val SPARK_PROPS_ENABLING_ICEBERG = Map(
    "spark.sql.extensions" -> ".*IcebergSparkSessionExtensions.*"
  )

  // For Iceberg, RAPIDS only supports running against the Hadoop filesystem catalog.
  private val SUPPORTED_CATALOGS = Set("hadoop")

  val EXEC_APPEND_DATA: String = "AppendData"
  // A Map between the spark node name and the SupportedOpStub.
  // Note that AppendDataExec is not supported for Iceberg.
  val DEFINED_EXECS: Map[String, SupportedOpStub] = Map(
    EXEC_APPEND_DATA ->
      SupportedOpStub(
        EXEC_APPEND_DATA,
        // The writeOp is not supported in Iceberg
        isSupported = false,
        opType = Option(OpTypes.WriteExec)
      )
  )

  /**
   * Checks if the properties indicate that the application is using Iceberg.
   * This can be checked by looking for keywords in one of the keys defined in
   * SPARK_PROPS_ENABLING_ICEBERG or if any spark catalog is set.
   *
   * @param properties spark properties captured from the eventlog environment details
   * @return true if the properties indicate that it is an Iceberg app.
   */
  def isIcebergEnabled(properties: collection.Map[String, String]): Boolean = {
    SPARK_PROPS_ENABLING_ICEBERG.exists { case (key, value) =>
      properties.get(key).exists(_.matches(value))
    } || properties.keys.exists(key => SPARK_CATALOG_REGEX.pattern.matcher(key).matches())
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
