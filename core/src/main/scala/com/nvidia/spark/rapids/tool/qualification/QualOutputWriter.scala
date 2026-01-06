/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION.
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

import com.nvidia.spark.rapids.tool.planparser.{ExecInfo, PlanInfo}
import com.nvidia.spark.rapids.tool.profiling.ProfileUtils.replaceDelimiter
import org.apache.hadoop.conf.Configuration
import org.json4s.DefaultFormats

import org.apache.spark.sql.rapids.tool.util._

/**
 * This class handles the output files for qualification.
 * It can write both a raw csv file and then a text summary report.
 *
 * @param outputDir The directory to output the files to
 * @param printStdout Indicates if the summary report should be printed to stdout as well
 * @param hadoopConf Optional Hadoop Configuration to use
 */
class QualOutputWriter(outputDir: String, printStdout: Boolean,
    hadoopConf: Option[Configuration] = None) {

  implicit val formats: DefaultFormats.type = DefaultFormats

}

object QualOutputWriter {
  val COUNT = "Count"
  val STATUS_REPORT_PATH_STR = "Event Log"
  val STATUS_REPORT_STATUS_STR = "Status"
  val STATUS_REPORT_APP_ID = "AppID"
  val STATUS_REPORT_DESC_STR = "Description"
  val CSV_DELIMITER = ","

  def formatSQLDescription(sqlDesc: String, maxSQLDescLength: Int,
      delimiter: String): String = {
    val escapedMetaStr =
      StringUtils.renderStr(sqlDesc, doEscapeMetaCharacters = true, maxLength = maxSQLDescLength)
    // should be a one for one replacement so length wouldn't be affected by this
    replaceDelimiter(escapedMetaStr, delimiter)
  }

  def flattenedExecs(execs: Seq[ExecInfo]): Seq[ExecInfo] = {
    // need to remove the WholeStageCodegen wrappers since they aren't actual
    // execs that we want to get timings of
    execs.flatMap { e =>
      if (e.isClusterNode) {
        e.children.getOrElse(Seq.empty)
      } else {
        e.children.getOrElse(Seq.empty) :+ e
      }
    }
  }


  def getAllExecsFromPlan(plans: Seq[PlanInfo]): Set[ExecInfo] = {
    val topExecInfo = plans.flatMap(_.execInfo)
    topExecInfo.flatMap { e =>
      e.children.getOrElse(Seq.empty) :+ e
    }.toSet
  }

}
