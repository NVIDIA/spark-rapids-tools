/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION.
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

import scala.collection.mutable

import com.nvidia.spark.rapids.tool.planparser.{BatchScanExecParser, DataWritingCommandExecParser, ReadParser}
import com.nvidia.spark.rapids.tool.planparser.iceberg.IcebergOps

import org.apache.spark.sql.rapids.tool.{AccumToStageRetriever, AppBase}
import org.apache.spark.sql.rapids.tool.plangraph.{SparkPlanGraph, SparkPlanGraphNode, ToolsPlanGraph}
import org.apache.spark.sql.rapids.tool.util.CacheablePropsHandler
import org.apache.spark.sql.rapids.tool.util.stubs.SparkPlanExtensions.{SparkPlanInfoOps, UpStreamSparkPlanInfoOps}
import org.apache.spark.sql.rapids.tool.util.stubs.SparkPlanInfo

/**
 * Represents a version of the SQLPlan holding the specific information related to that SQL plan.
 * In an AQE event, Spark sends the new update with the updated SparkPlanInfo and
 * PhysicalPlanDescription.
 * @param sqlId sql plan ID. This is redundant because the SQLPlan Model has the same information.
 *              However, storing it here makes the code easier as we don't have to build
 *              (sqlId, SQLPLanVersion) tuples.
 * @param version the version number of this plan. It starts at 0 and increments by 1 for each
 *                SparkListenerSQLAdaptiveExecutionUpdate.
 * @param planInfo The instance of SparkPlanInfo for this version of the plan. This is a stub
 *                 implementation provided by core-tools.
 * @param physicalPlanDescription The string representation of the physical plan for this version.
 * @param appInst the application instance the sqlPlan belongs to. This is used to
 *                extract some information and properties to help with parsing and operator
 *                extraction. For example, if application is Iceberg, deltaLake, etc.
 * @param isFinal Flag to indicate if this plan is the final plan for the SQLPlan
 */
class SQLPlanVersion(
    val sqlId: Long,
    val version: Int,
    val planInfo: SparkPlanInfo,
    val physicalPlanDescription: String,
    var appInst: AppBase,
    var isFinal: Boolean = true) {

  // Cache the spark conf provider to check for App configurations when needed.
  // This is set when the graph is built.
  private val _sparkConfigProvider: Option[CacheablePropsHandler] = Some(appInst)
  // Used to cache the Spark graph for that plan to avoid creating a plan.
  // This graph can be used and then cleaned up at the end of the execution.
  // This has to be accessed through the getToolsPlanGraph() which synchronizes on this object to
  // avoid races between threads.
  private var _sparkGraph: Option[ToolsPlanGraph] = None

  /**
   * Builds the ToolsPlanGraph for this plan version.
   * The graph is cached to avoid re-creating a sparkPlanGraph every time.
 *
   * @param accumStageMapper The AccumToStageRetriever used to find the stage for each accumulator.
   * @return the ToolsPlanGraph.
   */
  def buildSparkGraph(
      accumStageMapper: AccumToStageRetriever): ToolsPlanGraph = {
    this.synchronized {
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
   * This works by looping on all the nodes and filtering write execs where we can extract
   * metadata (table name, format, etc.).
   *
   * @return the list of write records for this plan if any.
   */
  private def initWriteOperationRecords(): Iterable[WriteOperationRecord] = {
    val checkIceberg = _sparkConfigProvider match {
      case Some(conf) => conf.isIcebergEnabled
      case None => false
    }
    // Pass physicalPlanDescription for ReplaceData/WriteDelta
    // metadata extraction for Iceberg.
    val physPlanOpt = Option(physicalPlanDescription).filter(_.nonEmpty)

    getToolsPlanGraph.allNodes
      .flatMap { node =>
        // Try Iceberg-specific extraction first.
        // - AppendData: extracted from simpleString
        // - ReplaceData/WriteDelta: extracted from physicalPlanDescription
        // - MergeRows: intentionally excluded (not a write operation)
        val metaFromIceberg = if (checkIceberg) {
          IcebergOps.extractOpMeta(node, _sparkConfigProvider, physPlanOpt)
        } else {
          None
        }
        val opMeta = metaFromIceberg match {
          case Some(_) => metaFromIceberg
          case _ =>
            // Fall back to the generic DataWritingCommandExecParser which handles
            // standard Spark write commands and DeltaLake.
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
    writeRecords.iterator.map(_.operationMeta.dataFormat()).toSet
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
    planInfo.getPlansWithSchema
  }

  /**
   * This is used to extract the metadata of ReadV1 nodes in Spark Plan Info
   * @param graph SparkPlanGraph to use.
   * @return all the read datasources V1 recursively that are read by this plan including.
   */
  private def getReadDSV1(graph: SparkPlanGraph): Iterable[DataSourceRecord] = {
    // Define a mutable set of scan nodes to avoid matching the same node multiple times.
    val scanNodes =
      mutable.LinkedHashSet[SparkPlanGraphNode]().++(graph.allNodes.filter(ReadParser.isScanNode))
    getPlansWithSchema.flatMap { plan =>
      val meta = plan.metadata
      // TODO: Improve the extraction of ReaSchema using RegEx (ReadSchema):\s(.*?)(\.\.\.|,\s|$)
      val readSchema =
        ReadParser.formatSchemaStr(meta.getOrElse(ReadParser.METAFIELD_TAG_READ_SCHEMA, ""))
      scanNodes.find { node =>
        // TODO: we should find a different way to match the PlanInfo to the GraphNode because
        //       there are some cases where the schema is empty. i.e, schema: struct<>, which
        //       implies that an empty string matches on everything.
        // Get ReadSchema of each Node and sanitize it for comparison
        val trimmedNode = AppBase.trimSchema(ReadParser.parseReadNode(node).schema)
        if (readSchema.isEmpty && trimmedNode.isEmpty) {
          true
        } else {
          readSchema.contains(trimmedNode)
        }
      } match {
        case Some(matchingNode) =>
          // remove the node from the set to avoid matching it again.
          scanNodes.remove(matchingNode)
          Some(DataSourceRecord(
            sqlId,
            version,
            matchingNode.id,
            ReadParser.extractTagFromV1ReadMeta(ReadParser.METAFIELD_TAG_FORMAT, meta),
            ReadParser.extractTagFromV1ReadMeta(ReadParser.METAFIELD_TAG_LOCATION, meta),
            ReadParser.extractTagFromV1ReadMeta(ReadParser.METAFIELD_TAG_PUSHED_FILTERS, meta),
            readSchema,
            ReadParser.extractTagFromV1ReadMeta(ReadParser.METAFIELD_TAG_DATA_FILTERS, meta),
            ReadParser.extractTagFromV1ReadMeta(ReadParser.METAFIELD_TAG_PARTITION_FILTERS, meta),
            fromFinalPlan = isFinal))
        case _ => None
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
      val res = if (BatchScanExecParser.accepts(node, _sparkConfigProvider)) {
        BatchScanExecParser.extractReadMetaData(node, _sparkConfigProvider)
      } else {
        ReadParser.parseReadNode(node)
      }
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
   * Builder for constructing SQLPlanVersion instances with validation.
   *
   * Provides a fluent API for setting required and optional parameters before building
   * the SQLPlanVersion. Ensures required fields (planInfo, physicalPlanDescription, appInst)
   * are set before construction.
   */
  class Builder {
    private var sqlId: Long = _
    private var version: Int = _
    private var planInfo: org.apache.spark.sql.execution.SparkPlanInfo = _
    private var physicalPlanDescription: String = _
    private var appInst: AppBase = _
    private var isFinal: Boolean = true

    /** Sets the SQL plan ID. */
    def withSqlId(sqlId: Long): Builder = {
      this.sqlId = sqlId
      this
    }

    /** Sets the plan version number (increments with each AQE update). */
    def withVersion(version: Int): Builder = {
      this.version = version
      this
    }

    /** Sets the SparkPlanInfo from upstream Spark execution. */
    def withPlanInfo(planInfo: org.apache.spark.sql.execution.SparkPlanInfo): Builder = {
      this.planInfo = planInfo
      this
    }

    /** Sets the physical plan description string. */
    def withPhysicalPlanDescription(physicalPlanDescription: String): Builder = {
      this.physicalPlanDescription = physicalPlanDescription
      this
    }

    /** Sets the application instance for context and configuration. */
    def withAppInst(appInst: AppBase): Builder = {
      this.appInst = appInst
      this
    }

    /** Sets whether this is the final plan version (default: true). */
    def withIsFinal(isFinal: Boolean): Builder = {
      this.isFinal = isFinal
      this
    }

    /**
     * Builds the SQLPlanVersion instance.
     *
     * Validates that required fields are set and converts the upstream SparkPlanInfo
     * to a platform-aware representation before construction.
     *
     * @return A new SQLPlanVersion instance.
     */
    @throws[java.lang.IllegalArgumentException]
    def build(): SQLPlanVersion = {
      require(planInfo != null, "planInfo must be set")
      require(physicalPlanDescription != null, "physicalPlanDescription must be set")
      require(appInst != null, "appInst must be set")

      new SQLPlanVersion(
        sqlId,
        version,
        planInfo.asPlatformAware(appInst),
        physicalPlanDescription,
        appInst,
        isFinal
      )
    }
  }

  /** Creates a new Builder instance for constructing SQLPlanVersion. */
  def builder: Builder = new Builder()
}
