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

package com.nvidia.spark.rapids.tool.tuning

import com.nvidia.spark.rapids.tool.profiling.AppSummaryInfoBaseProvider

import org.apache.spark.sql.rapids.tool.qualification.{QualificationAppInfo, QualificationSummaryInfo}

/**
 * Implementation of AppInfoPropertyGetter to wrap the output of the Qualification analysis.
 * @param appInfo the main QualificationAppInfo object representing the CPU application.
 * @param appAggStats optional stats aggregate is included here for future improvement as we may
 *                    need to feed the autotuner with values from the aggregates.
 */
class QualAppSummaryInfoProvider(
    val appInfo: QualificationAppInfo,
    val appAggStats: Option[QualificationSummaryInfo]) extends AppSummaryInfoBaseProvider {
  override def isAppInfoAvailable = true
  private def findPropertyInternal(
      key: String, props: collection.Map[String, String]): Option[String] = {
    props.get(key)
  }

  override def getAllProperties: Map[String, String] = {
    appInfo.sparkProperties
  }

  override def getSparkProperty(propKey: String): Option[String] = {
    findPropertyInternal(propKey, appInfo.sparkProperties)
  }

  override def getRapidsProperty(propKey: String): Option[String] = {
    getSparkProperty(propKey)
  }

  override def getSystemProperty(propKey: String): Option[String] = {
    findPropertyInternal(propKey, appInfo.systemProperties)
  }

  override def getSparkVersion: Option[String] = {
    Option(appInfo.sparkVersion)
  }

  def getAppID: String = appInfo.appId
}
