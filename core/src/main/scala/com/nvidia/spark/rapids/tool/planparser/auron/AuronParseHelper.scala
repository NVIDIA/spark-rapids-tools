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
package com.nvidia.spark.rapids.tool.planparser.auron

import scala.util.matching.Regex

import com.nvidia.spark.rapids.tool.plugins.{AppPropVersionExtractorFromCPTrait, PropConditionOnSparkExtTrait}

object AuronParseHelper extends PropConditionOnSparkExtTrait
    with AppPropVersionExtractorFromCPTrait {
  private val AURON_ENABLED_KEY = "spark.auron.enabled"
  private val AURON_ENABLED_KEY_DEFAULT = "true"
  // An Auron app is identified using the following properties from spark properties.
  override val extensionRegxMap = Map(
    "spark.sql.extensions" -> ".*AuronSparkSessionExtension.*"
  )

  override val cpKeyRegex: Regex = """auron-spark-[\w.-]+-(\d+\.\d+\.\d+)-incubating\.jar""".r

  override def eval(properties: collection.Map[String, String]): Boolean = {
    val auronExtensionExists = super.eval(properties)
    auronExtensionExists && isAuronTurnedOn(properties)
  }

  private def isAuronTurnedOn(properties: collection.Map[String, String]): Boolean = {
    properties.getOrElse(AURON_ENABLED_KEY, AURON_ENABLED_KEY_DEFAULT)
      .trim.equalsIgnoreCase(AURON_ENABLED_KEY_DEFAULT)
  }
}
