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

import scala.collection.{breakOut, immutable, mutable}

import org.apache.spark.sql.execution.SparkPlanInfo

/**
 * Container class to store the information about SqlPlans.
 */
class SQLPlanModelManager {
  // SortedMap is used to keep the order of the sqlPlans by Id
  val sqlPlans: mutable.SortedMap[Long, SQLPlanModel] = mutable.SortedMap[Long, SQLPlanModel]()

  /**
   * Get the SparkPlanInfo for the given executionId. This is the last plan version added to that
   * SQLPlan.
   * @param id executionId of the SqlPlan
   * @return SparkPlanInfo if exists
   */
  def getPlanInfoById(id: Long): Option[SparkPlanInfo] = {
    // returns the last plan added to that executionId if exists
    getPlanById(id).map(_.planInfo)
  }

  /**
   * Get the SQLPlanModel for the given executionId.
   * @param id executionId of the SqlPlan
   * @return SQLPlanModel if exists
   */
  def getPlanById(id: Long): Option[SQLPlanModel] = {
    // returns the last plan added to that executionId if exists
    sqlPlans.get(id)
  }

  /**
   * Get the physical plan description for the given executionId. This is the last plan version.
   * @param id executionId of the SqlPlan
   * @return physical plan description if exists
   */
  def getPhysicalPlanById(id: Long): Option[String] = {
    getPlanById(id).map(_.physicalPlanDesc)
  }

  /**
   * Add a new execution to the SQLPlanModelManager. This is called when a
   * SparkListenerSQLExecutionStart is triggered.
   * @param id executionId of the SqlPlan
   * @param planInfo SparkPlanInfo for the new version of the plan.
   * @param physicalDescription String representation of the physical plan for the new version.
   */
  def addNewExecution(id: Long, planInfo: SparkPlanInfo, physicalDescription: String): Unit = {
    // TODO: in future we should pass more arguments to this method to capture the common
    //  information of an SqlPlan (i.e., startTime,..etc))
    val planModel = sqlPlans.getOrElseUpdate(id, new SQLPlanModelPrimaryWithDSCaching(id))
    planModel.addPlan(planInfo, physicalDescription)
  }

  /**
   * Handles the AQE event by adding a new version of the plan to the SQLPlanModelManager and update
   * the references to the last version.
   * @param id executionId of the SqlPlan
   * @param planInfo SparkPlanInfo for the new version of the plan.
   * @param physicalDescription String representation of the physical plan for the new version.
   */
  def addAQE(id: Long, planInfo: SparkPlanInfo, physicalDescription: String): Unit = {
    // TODO: we can verify that the id already exists, but we ignore that verification now as it is
    //  undefined how we should handle an AQE without the original plan.
    addNewExecution(id, planInfo, physicalDescription)
  }

  /**
   * Apply a function to the SparkPlanInfo of the given executionId if it exists.
   * @param id executionId of the SqlPlan
   * @param f function to be applied
   * @tparam A Type of the result of the function
   * @return Some(result) if the plan exists, None otherwise
   */
  def applyToPlanInfo[A](id: Long)(f: SparkPlanInfo => A): Option[A] = {
    getPlanInfoById(id).map(f)
  }

  /**
   * Shortcuts to make the new implementation compatible with previous code that was expecting a
   * Map[Long, String]
   * @return map between executionId and the physical description of the last version.
   */
  def getPhysicalPlans: immutable.Map[Long, String] = {
    immutable.SortedMap[Long, String]() ++ sqlPlans.mapValues(_.physicalPlanDesc)
  }

  def getTruncatedPrimarySQLPlanInfo: Seq[SQLPlanInfoJsonWrapper] = {
    sqlPlans.map{
      case (sqlId, sparkPlanModel) =>
        SQLPlanInfoJsonWrapper(sqlId, sparkPlanModel.getPrimarySQLPlanInfo)
    }.toSeq
  }

  def remove(id: Long): Option[SQLPlanModel] = {
    sqlPlans.remove(id)
  }

  /**
   * When AQE is enabled, this methods is used to get the DataSources that are read by old versions
   * of the SQLPlan if any
   * @return Iterable of DataSourceRecord representing previous versions of the SQLPlan
   */
  def getDataSourcesFromOrigPlans: Iterable[DataSourceRecord] = {
    sqlPlans.values.flatMap(_.getDataSourcesFromOrigAQEPlans)
  }

  /**
   * Shortcut to make the new implementation compatible with previous code that was expecting a
   * Map[Long, SparkPlanInfo]
   * @return map between executionId and the SparkPlanVersion of the last version.
   */
  def getPlanInfos: immutable.Map[Long, SparkPlanInfo] = {
    immutable.SortedMap[Long, SparkPlanInfo]() ++ sqlPlans.mapValues(_.planInfo)
  }

  /**
   * Gets all the writeRecords of of the final plan of the SQL
   * @return Iterable of WriteOperationRecord representing the write operations.
   */
  def getWriteOperationRecords(): Iterable[WriteOperationRecord] = {
    sqlPlans.values.flatMap(_.plan.writeRecords)
  }

  /**
   * Converts the writeOperations into a String set to represent the format of the writeOps.
   * This only pulls the information from the final plan of the SQL.
   * @return a set of write formats
   */
  def getWriteFormats(): Set[String] = {
    sqlPlans.values.flatMap(_.plan.getWriteDataFormats)(breakOut)
  }
}
