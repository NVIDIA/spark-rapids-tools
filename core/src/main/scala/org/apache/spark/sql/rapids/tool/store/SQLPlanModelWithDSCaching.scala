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

/**
 * An extension of SQLPlanModel to only cache the DataSourceRecords from previous plan.
 * @param sqlId the executionID of the sqlPlan.
 */
class SQLPlanModelWithDSCaching(sqlId: Long) extends SQLPlanModel(sqlId) {
  // List that captures the DSV1Reads of previous PlanInfo versions excluding the most recent one.
  // This contains only the DataSourceRecords of previous plan excluding the
  private val cachedDataSources: ArrayBuffer[DataSourceRecord] = new ArrayBuffer[DataSourceRecord]()

  override def updatePlanField(newPlan: SQLPlanVersion): Unit = {
    plan = newPlan
  }

  override def resetPreviousPlan(): Unit = {
    // reset the final flag to initialize the dataSources correctly
    plan.resetFinalFlag()
    // cache the datasource records from previous plan if any
    cachedDataSources ++= plan.getAllReadDS
  }

  /**
   * Get all the DataSources from the original plans (excludes the most recent version).
   * @return Iterable of DataSourceRecord
   */
  override def getDataSourcesFromOrigAQEPlans: Iterable[DataSourceRecord] = {
    cachedDataSources
  }
}
