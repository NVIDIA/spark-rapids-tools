/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.tool.store

import com.nvidia.spark.rapids.tool.planparser.ReadParser

import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.rapids.tool.AppBase
import org.apache.spark.sql.rapids.tool.util.ToolsPlanGraph


class SQLPlanVersion(
    val sqlId: Long,
    val version: Int,
    val planInfo: SparkPlanInfo,
    val physicalPlanDescription: String,
    var isFinal: Boolean = true) {

  def resetFinalFlag(): Unit = {
    isFinal = false
  }
  def getPlansWithSchema: Seq[SparkPlanInfo] = {
    SQLPlanVersion.getPlansWithSchemaRecursive(planInfo)
  }
  // (ReadSchema):\s(.*?)(\.\.\.|,\s|$)
  def getDataSources: Iterable[DataSourceRecord] = {
    val planGraph = ToolsPlanGraph(planInfo)
    getPlansWithSchema.flatMap { plan =>
      val meta = plan.metadata
      val readSchema =
        ReadParser.formatSchemaStr(meta.getOrElse(ReadParser.METAFIELD_TAG_READ_SCHEMA, ""))
      val scanNodes = planGraph.allNodes.filter(ReadParser.isScanNode).filter(node => {
        // Get ReadSchema of each Node and sanitize it for comparison
        val trimmedNode = AppBase.trimSchema(ReadParser.parseReadNode(node).schema)
        readSchema.contains(trimmedNode)
      })
      if (scanNodes.nonEmpty) {
        Some(DataSourceRecord(
          sqlId,
          version,
          scanNodes.head.id,
          ReadParser.extractTagFromV1ReadMeta(ReadParser.METAFIELD_TAG_FORMAT, meta),
          ReadParser.extractTagFromV1ReadMeta(ReadParser.METAFIELD_TAG_LOCATION, meta),
          ReadParser.extractTagFromV1ReadMeta(ReadParser.METAFIELD_TAG_PUSHED_FILTERS, meta),
          readSchema,
          ReadParser.extractTagFromV1ReadMeta(ReadParser.METAFIELD_TAG_DATA_FILTERS, meta),
          ReadParser.extractTagFromV1ReadMeta(ReadParser.METAFIELD_TAG_PARTITION_FILTERS, meta),
          fromFinalPlan=isFinal))
      } else {
        None
      }
    }
  }
}

object SQLPlanVersion {
  private def getPlansWithSchemaRecursive(planInfo: SparkPlanInfo): Seq[SparkPlanInfo] = {
    val childRes = planInfo.children.flatMap(getPlansWithSchemaRecursive)
    if (planInfo.metadata != null &&
      planInfo.metadata.contains(ReadParser.METAFIELD_TAG_READ_SCHEMA)) {
      childRes :+ planInfo
    } else {
      childRes
    }
  }
}
