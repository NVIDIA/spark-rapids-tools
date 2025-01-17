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

abstract class DataSourceRecord {
  def sqlID: Long
  def version: Int
  def nodeId: Long
  def format: String
  def location: String
  def pushedFilters: String
  def schema: String
  def dataFilters: String
  def partitionFilters: String
  def isFromFinalPlan: Boolean
  def comments: String
}

case class NonFinalDataSourceCase(
    sqlID: Long,
    version: Int,
    nodeId: Long,
    format: String,
    location: String,
    pushedFilters: String,
    schema: String,
    dataFilters: String,
    partitionFilters: String) extends DataSourceRecord {

  override def isFromFinalPlan: Boolean = false
  override def comments: String = DataSourceRecord.COMMENT_NON_FINAL_PLAN
}

case class FinalDataSourceCase(
    sqlID: Long,
    version: Int,
    nodeId: Long,
    format: String,
    location: String,
    pushedFilters: String,
    schema: String,
    dataFilters: String,
    partitionFilters: String) extends DataSourceRecord {

  override def isFromFinalPlan: Boolean = true
  override def comments: String = DataSourceRecord.COMMENT_FINAL_PLAN
}

object DataSourceRecord {
  val COMMENT_NON_FINAL_PLAN = "isFinalPlan=false"
  val COMMENT_FINAL_PLAN = "isFinalPlan=true"

  def apply(
      sqlID: Long,
      version: Int,
      nodeId: Long,
      format: String,
      location: String,
      pushedFilters: String,
      schema: String,
      dataFilters: String,
      partitionFilters: String,
      fromFinalPlan: Boolean = true): DataSourceRecord = {
    fromFinalPlan match {
      case true => FinalDataSourceCase(
        sqlID,
        version,
        nodeId,
        format,
        location,
        pushedFilters,
        schema,
        dataFilters,
        partitionFilters)
      case _ => NonFinalDataSourceCase(
        sqlID,
        version,
        nodeId,
        format,
        location,
        pushedFilters,
        schema,
        dataFilters,
        partitionFilters)
    }
  }
}
