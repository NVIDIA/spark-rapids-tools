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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.execution.SparkPlanInfo

class SQLPlanModel(val id: Long) {
  val planVersions: ArrayBuffer[SQLPlanVersion] = new ArrayBuffer[SQLPlanVersion]()
  // This is not defined as option() because it should not happen that a SQLPlanModel is defined
  // without adding a plan. This is to cache the value of planVersions.last
  var plan: SQLPlanVersion = _

  def planInfo = plan.planInfo
  def physicalPlanDesc = plan.physicalPlanDescription

  private def updateCurrentPlan(newPlan: SQLPlanVersion): Unit = {
    planVersions += newPlan
    if (planVersions.size > 1) {
      plan.resetFinalFlag()
    }
    plan = planVersions.last
  }

  def addPlan(planInfo: SparkPlanInfo, physicalPlanDescription: String): Unit = {
    val planVersion = new SQLPlanVersion(id, planVersions.size, planInfo, physicalPlanDescription)
    updateCurrentPlan(planVersion)
  }

  def getDataSources: Iterable[DataSourceRecord] = {
    plan.getDataSources
  }

  def getDataSourcesFromOriAQEPlans: Iterable[DataSourceRecord] = {
    planVersions.dropRight(1).flatMap(_.getDataSources)
  }
}
