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

package com.nvidia.spark.rapids.tool

import com.nvidia.spark.rapids.tool.analysis.AggRawMetricsResult
import com.nvidia.spark.rapids.tool.profiling.{AppInfoJobStageAggMetricsVisitor, AppInfoPropertyGetter, AppInfoReadMetrics, AppInfoSqlTaskAggMetricsVisitor, AppInfoSQLTaskInputSizes, ApplicationSummaryInfo, DataSourceProfileResult, SingleAppSummaryInfoProvider}
import com.nvidia.spark.rapids.tool.tuning.QualAppSummaryInfoProvider

import org.apache.spark.sql.rapids.tool.ToolUtils
import org.apache.spark.sql.rapids.tool.qualification.{QualificationAppInfo, QualificationSummaryInfo}

/**
 * A base class definition that provides an empty implementation of an
 * applications summary information that is used for tuning the application.
 */
class AppSummaryInfoBaseProvider extends AppInfoPropertyGetter
  with AppInfoJobStageAggMetricsVisitor
  with AppInfoSqlTaskAggMetricsVisitor
  with AppInfoSQLTaskInputSizes
  with AppInfoReadMetrics {
  def isAppInfoAvailable = false
  override def getAllProperties: Map[String, String] = Map[String, String]()
  override def getSparkProperty(propKey: String): Option[String] = None
  override def getRapidsProperty(propKey: String): Option[String] = None
  override def getSystemProperty(propKey: String): Option[String] = None
  override def getProperty(propKey: String): Option[String] = {
    if (propKey.startsWith(ToolUtils.PROPS_RAPIDS_KEY_PREFIX)) {
      getRapidsProperty(propKey)
    } else if (propKey.startsWith("spark")){
      getSparkProperty(propKey)
    } else {
      getSystemProperty(propKey)
    }
  }
  override def getSparkVersion: Option[String] = None
  override def getMaxInput: Double = 0.0
  override def getMeanInput: Double = 0.0
  override def getMeanShuffleRead: Double = 0.0
  override def getJvmGCFractions: Seq[Double] = Seq()
  override def getSpilledMetrics: Seq[Long] = Seq()
  override def getShuffleStagesWithPosSpilling: Set[Long] = Set()
  override def getShuffleSkewStages: Set[Long] = Set()
  override def getRapidsJars: Seq[String] = Seq()
  override def getDistinctLocationPct: Double = 0.0
  override def getRedundantReadSize: Long = 0
}


object AppSummaryInfoBaseProvider {
  def fromAppInfo(appInfoInst: Option[ApplicationSummaryInfo]): AppSummaryInfoBaseProvider = {
    appInfoInst match {
      case Some(appSummaryInfo) => new SingleAppSummaryInfoProvider(appSummaryInfo)
      case _ => new AppSummaryInfoBaseProvider()
    }
  }

  /**
   * Constructs an application information provider based on the results of Qualification
   * tool.
   * @param appInfo
   * @param appAggStats optional aggregate of application stats
   * @return object that can be used by the AutoTuner to calculate the recommendations
   */
  def fromQualAppInfo(appInfo: QualificationAppInfo,
      appAggStats: Option[QualificationSummaryInfo] = None,
      rawAggMetrics: AggRawMetricsResult,
      dsInfo: Seq[DataSourceProfileResult]): AppSummaryInfoBaseProvider = {
    new QualAppSummaryInfoProvider(appInfo, appAggStats, rawAggMetrics, dsInfo)
  }
}
