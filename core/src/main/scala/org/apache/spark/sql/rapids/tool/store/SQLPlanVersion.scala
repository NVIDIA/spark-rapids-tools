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


/**
 * Represents a version of the SQLPlan holding the specific information related to that SQL plan.
 * In an AQE event, Spark sends the new update with the updated SparkPlanInfo and
 * PhysicalPlanDescription.
 * @param sqlId sql plan ID. This is redundant because the SQLPlan Model has the same information.
 *              However, storing it here makes the code easier as we don't have to build
 *              (sqlId, SQLPLanVersion) tuples.
 * @param version the version number of this plan. It starts at 0 and increments by 1 for each
 *                SparkListenerSQLAdaptiveExecutionUpdate.
 * @param planInfo The instance of SparkPlanInfo for this version of the plan.
 * @param physicalPlanDescription The string representation of the physical plan for this version.
 * @param isFinal Flag to indicate if this plan is the final plan for the SQLPlan
 */
class SQLPlanVersion(
    val sqlId: Long,
    val version: Int,
    val planInfo: SparkPlanInfo,
    val physicalPlanDescription: String,
    var isFinal: Boolean = true) {

  def resetFinalFlag(): Unit = {
    // This flag depends on the AQE events sequence.
    // It does not set that field using the substring of the physicalPlanDescription
    // (isFinalPlan=true).
    // The consequences of this is that for incomplete eventlogs, the last PlanInfo to be precessed
    // is considered final.
    isFinal = false
  }

  /**
   * Starting with the SparkPlanInfo for this version, recursively get all the SparkPlanInfo within
   * the children that define a ReadSchema.
   * @return Sequence of SparkPlanInfo that have a ReadSchema attached to it.
   */
  def getPlansWithSchema: Seq[SparkPlanInfo] = {
    SQLPlanVersion.getPlansWithSchemaRecursive(planInfo)
  }

  /**
   * This is used to extract the metadata of ReadV1 nodes in Spark Plan Info
   * @return all the read datasources V1 recursively that are read by this plan including.
   */
  def getReadDSV1: Iterable[DataSourceRecord] = {
    val planGraph = ToolsPlanGraph(planInfo)
    getPlansWithSchema.flatMap { plan =>
      val meta = plan.metadata
      // TODO: Improve the extraction of ReaSchema using RegEx (ReadSchema):\s(.*?)(\.\.\.|,\s|$)
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
  /**
   * Recursive call to get all the SparkPlanInfo that have a schema attached to it.
   * This is mainly used for V1 ReadSchema
   * @param planInfo The SparkPlanInfo to start the search from
   * @return A list of SparkPlanInfo that have a schema attached to it.
   */
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
