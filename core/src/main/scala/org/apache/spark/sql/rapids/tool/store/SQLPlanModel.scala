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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.execution.SparkPlanInfo

/**
 * SQLPlanModel is a class to store the information of a SQL Plan including all its versions.
 * A new instance of this class is created while handling SparkListenerSQLExecutionStart event.
 * Note that keeping track of all SQLPlanInfo versions could result in a large memory overhead.
 * See SQLPlanModelWithDSCaching for a more memory efficient version that only captures the
 * DataSourceRecords of a planInfo.
 * Therefore, it is recommended to:
 * 1- Use this class iff you need to keep track of all information inside all versions of a SQLPlan.
 * 2- Do some memory optimizations to reduce the overhead of storing all versions.
 * @param id SqlId
 */
class SQLPlanModel(val id: Long) {
  // TODO: common information related to the SQLPlan should be added as fields in this class.
  //       For example, the information tracked in
  //       AppBase.sqlIdToInfo Map[sqlId, SQLExecutionInfoClass] should belong here.
  // List of all the versions of a SQL Plan. For each AQE event, the new version is added to this
  // list.
  private val planVersions: ArrayBuffer[SQLPlanVersion] = new ArrayBuffer[SQLPlanVersion]()
  // A shortcut to the most recent version added to the list.
  // This is not defined as option() because it should not happen that a SQLPlanModel is defined
  // without adding a plan. This is to cache the value of planVersions.last
  var plan: SQLPlanVersion = _
  // Number of versions defined for this SQL Plan (i.e., 1 + [count of AQE update events])
  var versionsCount: Int = 0

  /**
   * A shortcut to the planInfo to abstract the internal details of the SQLPlanVersion.
   * @return SparkPlanInfo of the latest version of the plan.
   */
  def planInfo = plan.planInfo

  /**
   * A shortcut to the physicalPlanDescription to abstract the internal details of the
   * SQLPlanVersion.
   * @return physicalPlanDesc of the latest version of the plan.
   */
  def physicalPlanDesc = plan.physicalPlanDescription

  /**
   * Public method to add a new version of the plan.
   * @param planInfo SparkPlanInfo for the new version of the plan.
   * @param physicalPlanDescription String representation of the physical plan for the new version.
   */
  def addPlan(planInfo: SparkPlanInfo, physicalPlanDescription: String): Unit = {
    // By default, a new planVersion is defined as final.
    val planVersion = new SQLPlanVersion(id, versionsCount, planInfo, physicalPlanDescription)
    // Update references and shortcuts to the latest plan and cache previous one if any
    updateVersions(planVersion)
  }

  protected def updatePlanField(newPlan: SQLPlanVersion): Unit = {
    planVersions += newPlan
    plan = newPlan
  }

  // After adding a new version, reset the previous plan if necessary
  protected def resetPreviousPlan(): Unit = {
    plan.resetFinalFlag()
    // call any cleanup code necessary for the plan
    plan.cleanUpPlan()
  }

  /**
   * Update the planVersions list and the plan shortcut to the latest version. It caches the
   * previous information if any.
   * @param newPlan the latest version of the plan.
   */
  private def updateVersions(newPlan: SQLPlanVersion): Unit = {
    // increment the versionsCount
    versionsCount += 1
    if (versionsCount > 1) {
      // reset the flag of the previous Version
      resetPreviousPlan()
    }
    // Update the shortcuts to the latest version
    updatePlanField(newPlan)
  }

  /**
   * Get the DataSources from the most recent version of the plan.
   * @return Iterable of DataSourceRecord
   */
  def getDataSources: Iterable[DataSourceRecord] = {
    plan.getAllReadDS
  }

  /**
   * Get all the DataSources from the original plans (excludes the most recent version).
   * @return Iterable of DataSourceRecord
   */
  def getDataSourcesFromOrigAQEPlans: Iterable[DataSourceRecord] = {
    // TODO: Consider iterating on the node to add DSV2 as well.
    planVersions.dropRight(1).flatMap(_.getAllReadDS)
  }

  /**
   * Get the primary SQLPlanInfo, which is the first version of the plan.
   * In case no SQL version has been added, it should return None.
   * Default scenarios warrant that in case a SQLPlanModel is created, it should have at least
   * one version.
   * @return Option containing the primary SparkPlanInfo if it exists.
   */
  def getPrimarySQLPlanInfo: Option[SparkPlanInfo] = {
    planVersions.find(_.version == 0).map(_.planInfo)
  }
}

/**
 * This JSON wrapper is used to serialize the SQLPlanInfo to a JSON format
 * in a truncated form. We don't need to serialize the entire SparkPlanInfo
 * Just the nodeName, simpleString and children.
 * @param sqlId The id associated with the SQLPlanInfo
 * @param sparkPlanInfo The SparkPlanInfo in a truncated form
 */
case class SQLPlanInfoJsonWrapper(
     sqlId: Long,
     sparkPlanInfo: SQLPlanInfoJsonWrapper.SparkPlanInfoTruncated)

object SQLPlanInfoJsonWrapper {

  // This SparkPlanInfoTruncated is used to serialize the SparkPlanInfo by removing
  // the unnecessary fields. Only three fields are kept:
  // 1. nodeName
  // 2. simpleString
  // 3. children - which is a list of SparkPlanInfoTruncated (calculated recursively)
  case class SparkPlanInfoTruncated(
     nodeName: String,
     simpleString: String,
     children: Seq[SparkPlanInfoTruncated])

  def apply(sqlId: Long, sparkPlanInfo: SparkPlanInfo): SQLPlanInfoJsonWrapper = {
    SQLPlanInfoJsonWrapper(sqlId, truncateSparkPlanInfo(sparkPlanInfo))
  }

  // This function is used to recursively create a truncated version of the SparkPlanInfo
  // by removing the unnecessary fields for the root SparkPlanInfo node
  // as well as its children.
  private def truncateSparkPlanInfo(info: SparkPlanInfo): SparkPlanInfoTruncated = {
    SparkPlanInfoTruncated(info.nodeName,
      info.simpleString,
      info.children.map(truncateSparkPlanInfo))
  }
}
