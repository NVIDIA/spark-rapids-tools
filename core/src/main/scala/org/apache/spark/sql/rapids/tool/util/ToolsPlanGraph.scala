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

package org.apache.spark.sql.rapids.tool.util

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable
import scala.reflect.runtime.universe._

import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.{SparkPlanGraph, SparkPlanGraphCluster, SparkPlanGraphEdge, SparkPlanGraphNode, SQLPlanMetric}
import org.apache.spark.sql.rapids.tool.store.AccumNameRef

class DBReflectionEntry[T](mirror: Mirror, className: String, paramsSize: Option[Int] = None) {
  // Get the class symbol
  private val classSymbol = mirror.staticClass(className)
  private val reflectiveClass = mirror.reflectClass(classSymbol)
  // Get the constructor method symbol
  val constr: MethodSymbol = createConstructor(classSymbol, paramsSize)

  // If the paramsCount is defined, we select the the constructor that has parameters size equal to
  // that value
  private def createConstructor(symbol: ClassSymbol, paramsCount: Option[Int]): MethodSymbol = {
    paramsCount match {
      case None =>
        // return the primary constructor
        symbol.primaryConstructor.asMethod
      case Some(count) =>
        // return the constructor with given  parameter size
        val constructors = symbol.info.decls.filter(_.isConstructor)
          .map(_.asMethod)
          .filter(_.paramLists.flatten.size == count)
        val constructor = constructors.headOption.getOrElse {
          throw new IllegalArgumentException(
            s"No constructor found with exactly $count parameters for class[$className]")
        }
        constructor
    }
  }

  def createInstanceFromList(args: List[_]): T = {
    reflectiveClass
      .reflectConstructor(constr)(args: _*)
      .asInstanceOf[T]
  }
}

case class DBGraphNodeStub(m: Mirror)
  extends DBReflectionEntry[org.apache.spark.sql.execution.ui.SparkPlanGraphNode](
    m, "org.apache.spark.sql.execution.ui.SparkPlanGraphNode") {
  // DataBricks has different constructor of the sparkPlanGraphNode
  // [(long,java.lang.String,java.lang.String,scala.collection.Seq,java.lang.String,
  // boolean,scala.Option,scala.Option)] and
  // [final long id, final java.lang.String name, final java.lang.String desc,
  // final scala.collection.Seq<org.apache.spark.sql.execution.ui.SQLPlanMetric> metrics,
  // final java.lang.String rddScopeId, final boolean started,
  // final scala.Option<scala.math.BigInt> estRowCount)

  // For 10.4 --> only 1 constructor and has 6 arguments (not 7)
  // (final long id, final java.lang.String name, final java.lang.String desc,
  // final scala.collection.Seq<org.apache.spark.sql.execution.ui.SQLPlanMetric> metrics,
  // final java.lang.String rddScopeId, final scala.Option<scala.math.BigInt> estRowCount

  // DB10.4 has constructor with 6 arguments.
  private val isDB104OrOlder: Boolean = constr.paramLists.flatten.size < 7

  def createInstance(id: Long, name: String, desc: String,
      metrics: collection.Seq[SQLPlanMetric]): SparkPlanGraphNode = {
    // Define argument values
    val argValues = if (isDB104OrOlder) {
      List(id, name, desc, metrics, "", None)
    } else {
      List(id, name, desc, metrics, "", false, None, None)
    }
    createInstanceFromList(argValues)
  }
}

case class DBGraphSQLMetricStub(m: Mirror)
  extends DBReflectionEntry[org.apache.spark.sql.execution.ui.SQLPlanMetric](
    m, "org.apache.spark.sql.execution.ui.SQLPlanMetric") {
  // DataBricks has different constructor of the sparkPlanGraphNode
  //Array(final java.lang.String name, final long accumulatorId,
  // final java.lang.String metricType, final boolean experimental)

  // for 10.4 it is only one constructor with 3 arguments.
  // final java.lang.String name, final long accumulatorId, final java.lang.String metricType
  private val isDB104OrOlder: Boolean = constr.paramLists.flatten.size < 4
  def createInstance(name: String,
      accumulatorId: Long,
      metricType: String): SQLPlanMetric = {
    val argValues = if (isDB104OrOlder) {
      List(name, accumulatorId, metricType)
    } else {
      List(name, accumulatorId, metricType, false)
    }
    createInstanceFromList(argValues)
  }
}

case class DBGraphClusterStub(m: Mirror)
  extends DBReflectionEntry[org.apache.spark.sql.execution.ui.SparkPlanGraphCluster](
    m, "org.apache.spark.sql.execution.ui.SparkPlanGraphCluster") {
  // DataBricks has different constructor of the sparkPlanGraphNode
  // (final long id, final java.lang.String name, final java.lang.String desc,
  // final ArrayBuffer<org.apache.spark.sql.execution.ui.SparkPlanGraphNode> nodes,
  // final scala.collection.Seq<org.apache.spark.sql.execution.ui.SQLPlanMetric> metrics,
  // final java.lang.String rddScopeId)

  // 10.4 is the same as other versions
  // (final long id, final java.lang.String name, final java.lang.String desc,
  // final ArrayBuffer<org.apache.spark.sql.execution.ui.SparkPlanGraphNode> nodes,
  // final Seq<org.apache.spark.sql.execution.ui.SQLPlanMetric> metrics,
  // final java.lang.String rddScopeId
  def createInstance(id: Long,
      name: String,
      desc: String,
      nodes: mutable.ArrayBuffer[SparkPlanGraphNode],
      metrics: collection.Seq[SQLPlanMetric]): SparkPlanGraphCluster = {
    val argValues = List(id, name, desc, nodes, metrics, "")
    createInstanceFromList(argValues)
  }
}

// All versions in DB accept 2 parameters for constructor. So we stick to that version
// by passing (2) to the parent class
case class DBGraphEdgeStub(m: Mirror)
  extends DBReflectionEntry[org.apache.spark.sql.execution.ui.SparkPlanGraphEdge](
    m, "org.apache.spark.sql.execution.ui.SparkPlanGraphEdge", Option(2)) {
  // DataBricks has different constructor of the sparkPlanGraphNode
  // (final long fromId, final long toId,
  // final scala.Option<java.lang.Object> numOutputRowsId)
  //
  // for 10.4 only one constructor with two arguments
  // final long fromId, final long toId)

  def createInstance(fromId: Long, toId: Long): SparkPlanGraphEdge = {
    val argValues = List(fromId, toId)
    createInstanceFromList(argValues)
  }
}

// Container class to hold snapshot of the reflection fields instead of recalculating them every
// time we call the constructor
case class DBReflectionContainer() {
  private val mirror = runtimeMirror(getClass.getClassLoader)
  private val nodeStub = DBGraphNodeStub(mirror)
  private val clusterStub = DBGraphClusterStub(mirror)
  private val edgeStub = DBGraphEdgeStub(mirror)
  private val metricStub = DBGraphSQLMetricStub(mirror)

  def constructNode(id: Long, name: String, desc: String,
      metrics: collection.Seq[SQLPlanMetric]): SparkPlanGraphNode = {
    nodeStub.createInstance(id, name, desc, metrics)
  }

  def constructSQLPlanMetric(name: String,
      accumulatorId: Long,
      metricType: String): SQLPlanMetric = {
    metricStub.createInstance(name, accumulatorId, metricType)
  }

  def constructCluster(id: Long,
      name: String,
      desc: String,
      nodes: mutable.ArrayBuffer[SparkPlanGraphNode],
      metrics: collection.Seq[SQLPlanMetric]): SparkPlanGraphCluster = {
    clusterStub.createInstance(id, name, desc, nodes, metrics)
  }

  def constructEdge(fromId: Long, toId: Long): SparkPlanGraphEdge = {
    edgeStub.createInstance(fromId, toId)
  }
}

/**
 * This code is mostly copied from org.apache.spark.sql.execution.ui.SparkPlanGraph
 * with changes to handle GPU nodes. Without this special handle, the default SparkPlanGraph
 * would not be able to recognize reused/exchange nodes leading to duplicating nodes.
 *
 * Build a SparkPlanGraph from the root of a SparkPlan tree.
 */
object ToolsPlanGraph {
  // TODO: We should have a util to detect if the runtime is Databricks.
  //      This can be achieved by checking for spark properties
  //      spark.databricks.clusterUsageTags.clusterAllTags
  private lazy val dbRuntimeReflection = DBReflectionContainer()
  // By default call the Spark constructor. If this fails, we fall back to the DB constructor
  def constructGraphNode(id: Long, name: String, desc: String,
      metrics: collection.Seq[SQLPlanMetric]): SparkPlanGraphNode = {
    try {
      new SparkPlanGraphNode(id, name, desc, metrics, 0)
    } catch {
      case _: java.lang.NoSuchMethodError =>
        dbRuntimeReflection.constructNode(id, name, desc, metrics)
    }
  }

  private def constructSQLPlanMetric(name: String,
      accumulatorId: Long,
      metricType: String): SQLPlanMetric = {
    val accNameRef = AccumNameRef.getOrCreateAccumNameRef(name)
    try {
      SQLPlanMetric(accNameRef.value, accumulatorId, metricType)
    } catch {
      case _: java.lang.NoSuchMethodError =>
        dbRuntimeReflection.constructSQLPlanMetric(
          accNameRef.value, accumulatorId, metricType)
    }
  }

  private def constructCluster(id: Long,
      name: String,
      desc: String,
      nodes: mutable.ArrayBuffer[SparkPlanGraphNode],
      metrics: collection.Seq[SQLPlanMetric]): SparkPlanGraphCluster = {
    try {
      new SparkPlanGraphCluster(id, name, desc, nodes, metrics, 0)
    } catch {
      case _: java.lang.NoSuchMethodError =>
        dbRuntimeReflection.constructCluster(id, name, desc, nodes, metrics)
    }
  }
  private def constructEdge(fromId: Long, toId: Long): SparkPlanGraphEdge = {
    try {
      SparkPlanGraphEdge(fromId, toId)
    } catch {
      case _: java.lang.NoSuchMethodError =>
        dbRuntimeReflection.constructEdge(fromId, toId)
    }
  }

  /**
   * Build a SparkPlanGraph from the root of a SparkPlan tree.
   */
  def apply(planInfo: SparkPlanInfo): SparkPlanGraph = {
    try {
      val nodeIdGenerator = new AtomicLong(0)
      val nodes = mutable.ArrayBuffer[SparkPlanGraphNode]()
      val edges = mutable.ArrayBuffer[SparkPlanGraphEdge]()
      val exchanges = mutable.HashMap[SparkPlanInfo, SparkPlanGraphNode]()
      buildSparkPlanGraphNode(planInfo, nodeIdGenerator, nodes, edges, null, null, exchanges)
      new SparkPlanGraph(nodes, edges)
    } catch {
      // If the construction of the graph fails due to NoSuchMethod, then it is possible the
      // runtime is DB and we fallback to the loaded runtime jars
      case _ : java.lang.NoSuchMethodError | _ : java.lang.IllegalArgumentException =>
        SparkPlanGraph(planInfo)
    }
  }

  private def processPlanInfo(nodeName: String): String = {
    if (nodeName.startsWith("Gpu")) {
      nodeName.replaceFirst("Gpu", "")
    } else {
      nodeName
    }
  }

  private def buildSparkPlanGraphNode(
      planInfo: SparkPlanInfo,
      nodeIdGenerator: AtomicLong,
      nodes: mutable.ArrayBuffer[SparkPlanGraphNode],
      edges: mutable.ArrayBuffer[SparkPlanGraphEdge],
      parent: SparkPlanGraphNode,
      subgraph: SparkPlanGraphCluster,
      exchanges: mutable.HashMap[SparkPlanInfo, SparkPlanGraphNode]): Unit = {
    processPlanInfo(planInfo.nodeName) match {
      case name if name.startsWith("WholeStageCodegen") =>
        val metrics = planInfo.metrics.map { metric =>
          constructSQLPlanMetric(metric.name, metric.accumulatorId, metric.metricType)
        }

        val cluster = constructCluster(
          nodeIdGenerator.getAndIncrement(),
          planInfo.nodeName,
          planInfo.simpleString,
          mutable.ArrayBuffer[SparkPlanGraphNode](),
          metrics)
        nodes += cluster

        buildSparkPlanGraphNode(
          planInfo.children.head, nodeIdGenerator, nodes, edges, parent, cluster, exchanges)
      case "InputAdapter" =>
        buildSparkPlanGraphNode(
          planInfo.children.head, nodeIdGenerator, nodes, edges, parent, null, exchanges)
      case "BroadcastQueryStage" | "ShuffleQueryStage" =>
        if (exchanges.contains(planInfo.children.head)) {
          // Point to the re-used exchange
          val node = exchanges(planInfo.children.head)
          edges += constructEdge(node.id, parent.id)
        } else {
          buildSparkPlanGraphNode(
            planInfo.children.head, nodeIdGenerator, nodes, edges, parent, null, exchanges)
        }
      case "TableCacheQueryStage" =>
        buildSparkPlanGraphNode(
          planInfo.children.head, nodeIdGenerator, nodes, edges, parent, null, exchanges)
      case "Subquery" | "SubqueryBroadcast" if subgraph != null =>
        // Subquery should not be included in WholeStageCodegen
        buildSparkPlanGraphNode(planInfo, nodeIdGenerator, nodes, edges, parent, null, exchanges)
      case "Subquery" | "SubqueryBroadcast" if exchanges.contains(planInfo) =>
        // Point to the re-used subquery
        val node = exchanges(planInfo)
        edges += constructEdge(node.id, parent.id)
      case "ReusedSubquery" =>
        // Re-used subquery might appear before the original subquery, so skip this node and let
        // the previous `case` make sure the re-used and the original point to the same node.
        buildSparkPlanGraphNode(
          planInfo.children.head, nodeIdGenerator, nodes, edges, parent, subgraph, exchanges)
      case "ReusedExchange" if exchanges.contains(planInfo.children.head) =>
        // Point to the re-used exchange
        val node = exchanges(planInfo.children.head)
        edges += constructEdge(node.id, parent.id)
      case name =>
        val metrics = planInfo.metrics.map { metric =>
          constructSQLPlanMetric(metric.name, metric.accumulatorId, metric.metricType)
        }
        val node = constructGraphNode(nodeIdGenerator.getAndIncrement(),
          planInfo.nodeName, planInfo.simpleString, metrics)
        if (subgraph == null) {
          nodes += node
        } else {
          subgraph.nodes += node
        }
        if (name.contains("Exchange") || name.contains("Subquery")) {
          exchanges += planInfo -> node
        }

        if (parent != null) {
          edges += constructEdge(node.id, parent.id)
        }
        planInfo.children.foreach(
          buildSparkPlanGraphNode(_, nodeIdGenerator, nodes, edges, node, subgraph, exchanges))
    }
  }
}
