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

package com.nvidia.spark.rapids.tool.qualification

import com.nvidia.spark.rapids.tool.profiling.AppStatusResult
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.qualification.{QualAppMainSummary, QualificationSummaryInfo}
import org.apache.spark.sql.rapids.tool.util.RapidsToolsConfUtil


/**
 * The class is used to generate a QualToolResult from a seq of QualificationSummaryInfo
 * and a seq of AppStatusResult. The result is the main summary information of
 * the apps and the cluster status.
 */
class QualToolResultBuilder {
  /**
   * The map stores the main summary information of the apps. The key is the appId and
   * the value is the QualAppMainSummary.
   */
  private val appSummaries = new ConcurrentHashMap[String, QualAppMainSummary]()

  /**
   * This method is used to add a QualAppMainSummary using the QualificationSummaryInfo
   * to the builder.
   * It will return true if an app with the same appId already existed in the builder,
   * otherwise it will return false.
   *
   * @param summaryRec the QualificationSummaryInfo to be added
   * @return true if an app with the same appId already existed in the builder,
   *         otherwise it will return false
   */
  def appendAppSummary(summaryRec: QualificationSummaryInfo): Boolean = {
    val existedId = appSummaries.containsKey(summaryRec.appId)
    appSummaries.put(summaryRec.appId, QualAppMainSummary(summaryRec))
    existedId
  }

  /**
   * This method is used to generate a QualToolResult from the builder.
   * It will return a QualToolResult which contains the main summary information
   * of the apps and the cluster status.
   *
   * @param appStatuses the cluster status
   * @return a QualToolResult which contains the main summary information
   *         of the apps and the cluster status
   */
  def build(appStatuses: Seq[AppStatusResult]): QualToolResult = {
    // sort elements descending by opportunity and end-time
    val appSummaryList = appSummaries.values().asScala.toSeq.sortBy { rec =>
      (rec.estimatedInfo.gpuOpportunity, rec.startTime + rec.estimatedInfo.appDur)
    }.reverse
    QualToolResult(0, appSummaryList, appStatuses, None)
  }
}

/**
 * The class extends QualToolResultBuilder to capture the QualificationSummaryInfo entries till the
 * end of execution. This is mainly used for developing/testing purpose since the
 * QualificationSummaryInfo list can represent a huge memory overhead.
 */
class DetailedQualToolResultBuilder extends QualToolResultBuilder {
  private val detailedSummaries = new ConcurrentHashMap[String, QualificationSummaryInfo]()

  override def appendAppSummary(summaryRec: QualificationSummaryInfo): Boolean = {
    val existedId = super.appendAppSummary(summaryRec)
    detailedSummaries.put(summaryRec.appId, summaryRec)
    existedId
  }

  override def build(appStatuses: Seq[AppStatusResult]): QualToolResult = {
    val mainSummaryRes = super.build(appStatuses)
    // sort elements descending by opportunity and end-time
    val detailedSummaryList = detailedSummaries.values().asScala.toSeq.sortBy { rec =>
      (rec.estimatedInfo.gpuOpportunity, rec.startTime + rec.estimatedInfo.appDur)
    }.reverse
    mainSummaryRes.copy(appDetailedSummaries = Some(detailedSummaryList))
  }
}

object QualToolResultBuilder extends Logging {
  private def detailed(): DetailedQualToolResultBuilder = {
    logInfo("Building Detailed Report")
    new DetailedQualToolResultBuilder()
  }
  def apply(): QualToolResultBuilder = {
    // if test.qual.keep.per_app.summary is enabled load the detailed resukts that keep the
    // per-app full summary
    val detailsEnabled =
      RapidsToolsConfUtil.toolsBuildProperties
        .getProperty("test.qual.keep.per_app.summary", "false")
        .toBoolean
    if (detailsEnabled) {
      detailed()
    } else {
      new QualToolResultBuilder()
    }
  }
  // Return empty results without need to build
  def emptyResult(): QualToolResult = QualToolResult(0, Seq.empty, Seq.empty)
  // Return a failed result record without need to build
  def failedResult(): QualToolResult = QualToolResult(1, Seq.empty, Seq.empty)
}

/**
 * Represents the results of running a qualification tool.
 *
 * @param returnCode The return code of the tool
 * @param appSummaries A list of QualAppMainSummary objects summarizing the
 *                     results of each app
 * @param appStatus A list of AppStatusResult objects summarizing the status
 *                  of each app
 * @param appDetailedSummaries An optional list of QualificationSummaryInfo
 *                             objects providing detailed results for each app.
 *                             By default, this is an empty list. It will be set when build
 *                             properties file enables `test.qual.keep.per_app.summary`
 */
case class QualToolResult(
    returnCode: Int,
    appSummaries: Seq[QualAppMainSummary],
    appStatus: Seq[AppStatusResult],
    appDetailedSummaries: Option[Seq[QualificationSummaryInfo]] = None) {
  /**
   * Returns true if the tool failed, false otherwise.
   */
  def isFailed: Boolean = {
    returnCode != 0
  }

  /**
   * Returns the detailed summaries if available, or an empty list otherwise.
   */
  def detailedSummaries: Seq[QualificationSummaryInfo] = {
    appDetailedSummaries.getOrElse(Seq.empty)
  }
}
