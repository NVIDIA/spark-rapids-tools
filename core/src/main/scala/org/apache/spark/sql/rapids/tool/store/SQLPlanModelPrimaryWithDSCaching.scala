/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import org.apache.spark.sql.execution.SparkPlanInfo

/**
 * An extension of SQLPlanModelWithDSCaching to also cache the
 * primary plan version along with the DataSourceRecords from previous plan.
 * @param sqlId the executionID of the sqlPlan.
 */
class SQLPlanModelPrimaryWithDSCaching(sqlID: Long) extends SQLPlanModelWithDSCaching(sqlID) {

  private var cachedPrimaryPlanVersion: SQLPlanVersion = _

  override def updatePlanField(newPlan: SQLPlanVersion): Unit = {
    plan = newPlan
    if(cachedPrimaryPlanVersion == null) {
      cachedPrimaryPlanVersion = newPlan
    }
  }

  /**
   * This returns the primary plan version of the SQLPlanModel which
   * in this case is the cachedPrimaryPlanVersion.
   * @return Option containing the primary SparkPlanInfo if it exists.
   */
  override def getPrimarySQLPlanInfo: Option[SparkPlanInfo] = {
    Some(cachedPrimaryPlanVersion.planInfo)
  }
}
