/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

import scala.collection.breakOut

import com.nvidia.spark.rapids.tool.planparser.{DataWritingCommandExecParser, ReadParser}
import com.nvidia.spark.rapids.tool.planparser.iceberg.IcebergWriteOps

import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.SparkPlanGraph
import org.apache.spark.sql.rapids.tool.{AccumToStageRetriever, AppBase}
import org.apache.spark.sql.rapids.tool.util.{CacheablePropsHandler, ToolsPlanGraph}

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

  // Used to cache the Spark graph for that plan to avoid creating a plan.
  // This graph can be used and then cleaned up at the end of the execution.
  // This has to be accessed through the getToolsPlanGraph() which synchronizes on this object to
  // avoid races between threads.
  private var _sparkGraph: Option[ToolsPlanGraph] = None
  // Cache the spark conf provider to check for App configurations when needed.
  // This is set when the graph is built.
  private var _sparkConfigProvider: Option[CacheablePropsHandler] = None

  /**
   * Builds the ToolsPlanGraph for this plan version.
   * The graph is cached to avoid re-creating a sparkPlanGraph every time.
 *
   * @param accumStageMapper The AccumToStageRetriever used to find the stage for each accumulator.
   * @param sparkConfProvider An object that provides access to the Spark configuration of the
   *                            analyzed app.
   * @return the ToolsPlanGraph.
   */
  def buildSparkGraph(
      accumStageMapper: AccumToStageRetriever,
      sparkConfProvider: CacheablePropsHandler): ToolsPlanGraph = {
    this.synchronized {
      _sparkConfigProvider = Some(sparkConfProvider)
      _sparkGraph = Some(ToolsPlanGraph.createGraphWithStageClusters(planInfo, accumStageMapper))
      _sparkGraph.get
    }
  }

  /**
   * Returns the ToolsPlanGraph for this plan version. This method assumes that the graph has been
   * built. Any call to this method should be placed after the graph has been built.
   * @return the ToolsPlanGraph for this plan version.
   */
  def getToolsPlanGraph: ToolsPlanGraph = {
    this.synchronized {
      if (_sparkGraph.isEmpty) {
        throw new IllegalStateException("Spark graph is not initialized yet.")
      }
      _sparkGraph.get
    }
  }

  /**
   * Returns a spark graph object.
   * If the ToolsPlanGraph is initialized, it returns the wrapped spark-graph object.
   * In some cases (i.e., getting ReadSchema for all plan versions), the tools-graph is not
   * initialized yet. That's why we have to provide a fallback to create normal spark-graph without
   * stage clusters.
   * @return a SparkPlanGraph object.
   */
  private def getSparkPlanGraph: SparkPlanGraph = {
    this.synchronized {
      _sparkGraph match {
        case Some(graph) => graph.sparkGraph
        case None => ToolsPlanGraph(planInfo)
      }
    }
  }

  /**
   * Builds the list of write records for this plan.
   * This works by looping on all the nodes and filtering write execs.
   * @return the list of write records for this plan if any.
   */
  private def initWriteOperationRecords(): Iterable[WriteOperationRecord] = {
    // pick nodes that satisfies IcebergWriteOps.accepts
    val checkIceberg = _sparkConfigProvider match {
      case Some(conf) => conf.icebergEnabled
      case None => false
    }
    getToolsPlanGraph.allNodes
      // pick only nodes that marked as write Execs
      .flatMap { node =>
        // check Iceberg first
        val metaFromIcberg = if (checkIceberg) {
          IcebergWriteOps.extractOpMeta(node, _sparkConfigProvider)
        } else {
          None
        }
        val opMeta = metaFromIcberg match {
          case Some(_) => metaFromIcberg
          case _ =>
            // Fall back to the generic DataWritingCommandExecParser which handles DeltaLake too.
            if (DataWritingCommandExecParser.isWritingCmdExec(node.name.stripSuffix("$"))) {
              Option(DataWritingCommandExecParser.getWriteOpMetaFromNode(node))
            } else {
              None
            }
        }
        opMeta.map(m =>
          WriteOperationRecord(sqlId, version, node.id, operationMeta = m))
      }
  }

  // Captures the write operations for this plan. This is lazy because we do not need
  // to build this until we need it.
  lazy val writeRecords: Iterable[WriteOperationRecord] = {
    initWriteOperationRecords()
  }

  // Converts the writeRecords into a write formats.
  def getWriteDataFormats: Set[String] = {
    writeRecords.map(_.operationMeta.dataFormat())(breakOut)
  }

  /**
   * Reset any data structure that has been used to free memory.
   */
  def cleanUpPlan(): Unit = {
    this.synchronized {
      _sparkGraph = None
    }
  }

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
  private def getPlansWithSchema: Seq[SparkPlanInfo] = {
    SQLPlanVersion.getPlansWithSchemaRecursive(planInfo)
  }

  /**
   * This is used to extract the metadata of ReadV1 nodes in Spark Plan Info
   * @param graph SparkPlanGraph to use.
   * @return all the read datasources V1 recursively that are read by this plan including.
   */
  private def getReadDSV1(graph: SparkPlanGraph): Iterable[DataSourceRecord] = {
    getPlansWithSchema.flatMap { plan =>
      val meta = plan.metadata
      // TODO: Improve the extraction of ReaSchema using RegEx (ReadSchema):\s(.*?)(\.\.\.|,\s|$)
      val readSchema =
        ReadParser.formatSchemaStr(meta.getOrElse(ReadParser.METAFIELD_TAG_READ_SCHEMA, ""))
      val scanNodes = graph.allNodes.filter(ReadParser.isScanNode).filter(node => {
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
          fromFinalPlan = isFinal))
      } else {
        None
      }
    }
  }

  /**
   * Get all the DataSources that are read by this plan (V2).
   * @param graph SparkPlanGraph to use.
   * @return List of DataSourceRecord for all the V2 DataSources read by this plan.
   */
  private def getReadDSV2(graph: SparkPlanGraph): Iterable[DataSourceRecord] = {
    graph.allNodes.filter(ReadParser.isDataSourceV2Node).map { node =>
      val res = ReadParser.parseReadNode(node)
      DataSourceRecord(
        sqlId,
        version,
        node.id,
        res.format,
        res.location,
        res.pushedFilters,
        res.schema,
        res.dataFilters,
        res.partitionFilters,
        fromFinalPlan = isFinal)
    }
  }

  /**
   * Get all the DataSources that are read by this plan (V1 and V1).
   * @return Iterable of DataSourceRecord
   */
  def getAllReadDS: Iterable[DataSourceRecord] = {
    val planGraph = getSparkPlanGraph
    getReadDSV1(planGraph) ++ getReadDSV2(planGraph)
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
