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

package com.nvidia.spark.rapids.tool.profiling

import scala.collection.mutable

import com.nvidia.spark.rapids.tool.analysis.{AppAnalysisBase, SparkSQLPlanInfoVisitor, SQLPlanInfoContext}
import com.nvidia.spark.rapids.tool.planparser.delta.DeltaLakeHelper

import org.apache.spark.sql.rapids.tool.profiling.ApplicationInfo
import org.apache.spark.sql.rapids.tool.store.SQLPlanModel


/**
 * A context object that holds the classification information for a SQLPlan
 * @param sqlPIGEntry the SqlPlanInfoGraphEntry for the SQLPlan
 * @param deltaOpsNode the list of nodes that are classified as Delta metadata
 */
case class SQLPlanClassifierCtxt(
    sqlPIGEntry: SQLPlanModel,
    deltaOpsNode: mutable.ArrayBuffer[Long] = mutable.ArrayBuffer.empty)
  extends SQLPlanInfoContext(sqlPIGEntry)


/**
 * An implementation of SparkSQLPlanInfoVisitor that visits all the nodes of a SQLPlanInfo to
 * assign classifications to each SQLPlan
 * @param app the AppBase object to analyze
 */
class SQLPlanClassifier(app: ApplicationInfo)
  extends AppAnalysisBase(app) with SparkSQLPlanInfoVisitor[SQLPlanClassifierCtxt] {
  // A HashMap[category: String, SQLIDs Set[Long]] that holds the relation between specific
  // category/class to the SQLID
  // Note that for now we have only category "deltaOp", but this is subject to be extended in the
  // future to classify the SQLPlans.
  val sqlCategories: mutable.HashMap[String, mutable.LinkedHashSet[Long]] =
    mutable.HashMap("deltaOp" -> mutable.LinkedHashSet.empty)

  override def visitNode(
      sqlPlanCtxt: SQLPlanClassifierCtxt,
      node: org.apache.spark.sql.rapids.tool.plangraph.SparkPlanGraphNode
  ): Unit = {
    // Check if the node is a delta metadata operation
    val isDeltaLog = DeltaLakeHelper.isDeltaOpNode(sqlPlanCtxt.sqlPIGEntry,
      app.sqlManager.getPhysicalPlanById(sqlPlanCtxt.getSQLPIGEntry.id).get, node)
    if (isDeltaLog) {
      // if it is a Delta operation, add it to the list of Delta operations nodes
      sqlPlanCtxt.deltaOpsNode += node.id
    }
  }

  override def createPlanCtxtFromPIGEntry(
      sqlPIGEntry: SQLPlanModel): SQLPlanClassifierCtxt = {
    SQLPlanClassifierCtxt(sqlPIGEntry)
  }

  override def postWalkPlan(planCtxt: SQLPlanClassifierCtxt): Unit = {
    // After visiting all the nodes of a SQLPlan, decide on the classifications
    if (planCtxt.deltaOpsNode.nonEmpty) {
      // If at least one nodes is defined as Delta operations, then the entire SQLPlan is a Delta
      // operation
      // Note that we do not keep the nodes in a global variable because we do not that for now
      sqlCategories("deltaOp") += planCtxt.getSQLPIGEntry.id
    }
  }
}
