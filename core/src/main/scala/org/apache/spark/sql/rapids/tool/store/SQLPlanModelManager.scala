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

import scala.collection.immutable
import scala.collection.mutable

import org.apache.spark.sql.execution.SparkPlanInfo

class SQLPlanModelManager {
  // SortedMap is used to keep the order of the sqlPlans by Id
  val sqlPlans: mutable.SortedMap[Long, SQLPlanModel] = mutable.SortedMap[Long, SQLPlanModel]()

  def getPlanInfoById(id: Long): Option[SparkPlanInfo] = {
    // returns the last plan added to that executionId if exists
    getPlanById(id).map(_.planInfo)
  }

  def getPlanById(id: Long): Option[SQLPlanModel] = {
    // returns the last plan added to that executionId if exists
    sqlPlans.get(id)
  }

  def getPhysicalPlanById(id: Long): Option[String] = {
    getPlanById(id).map(_.physicalPlanDesc)
  }

  def addNewExecution(id: Long, planInfo: SparkPlanInfo, physicalDescription: String): Unit = {
    val planModel = sqlPlans.getOrElseUpdate(id, new SQLPlanModel(id))
    planModel.addPlan(planInfo, physicalDescription)
  }

  def addAQE(id: Long, planInfo: SparkPlanInfo, physicalDescription: String): Unit = {
    // TODO: we can verify that the id already exists, but we ignore that verification now as it is
    //  undefined how we should handle an AQE without the original plan.
    addNewExecution(id, planInfo, physicalDescription)
  }

  def applyToPlanInfo[A](id: Long)(f: SparkPlanInfo => A): Option[A] = {
    getPlanInfoById(id).map(f)
  }

  def getPhysicalPlans: immutable.Map[Long, String] = {
    immutable.SortedMap[Long, String]() ++ sqlPlans.mapValues(_.plan.physicalPlanDescription)
  }

  def remove(id: Long): Option[SQLPlanModel] = {
    sqlPlans.remove(id)
  }

  def getDataSourcesFromOrigPlans: Iterable[DataSourceRecord] = {
    sqlPlans.values.flatMap(_.getDataSourcesFromOriAQEPlans)
  }

  def getPlanInfos: immutable.Map[Long, SparkPlanInfo] = {
    immutable.SortedMap[Long, SparkPlanInfo]() ++ sqlPlans.mapValues(_.planInfo)
  }
}
