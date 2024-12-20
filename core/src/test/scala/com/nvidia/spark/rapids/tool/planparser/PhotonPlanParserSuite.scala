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

package com.nvidia.spark.rapids.tool.planparser

import com.nvidia.spark.rapids.tool.PlatformNames
import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker


class PhotonPlanParserSuite extends BasePlanParserSuite {

  // scalastyle:off line.size.limit
  // Test cases for Photon nodes. We could add more operators here.
  val photonOpTestCases: Seq[(String, String)] = Seq(
    "PhotonBroadcastNestedLoopJoin" -> "BroadcastNestedLoopJoin",  // Case: Photon specific parser
    "PhotonProject" -> "Project",                                  // Case: Fallback to Spark CPU parser
    "PhotonShuffleMapStage" ->  "WholeStageCodegen"                // Case: WholeStageCodegen operator
  )
  // scalastyle:on line.size.limit

  photonOpTestCases.foreach { case (photonName, sparkName) =>
    test(s"$photonName is parsed as Spark $sparkName") {
      val eventLog = s"$qualLogDir/nds_q88_photon_db_13_3.zstd"
      val pluginTypeChecker = new PluginTypeChecker()
      val app = createAppFromEventlog(eventLog, platformName = PlatformNames.DATABRICKS_AWS)
      assert(app.sqlPlans.nonEmpty)
      val parsedPlans = app.sqlPlans.map { case (sqlID, plan) =>
        SQLPlanParser.parseSQLPlan(app.appId, plan, sqlID, "", pluginTypeChecker, app)
      }
      val allExecInfo = getAllExecsFromPlan(parsedPlans.toSeq)
      val reader = allExecInfo.filter(_.exec.contains(sparkName))
      assert(reader.nonEmpty, s"Failed to find $sparkName in $allExecInfo")
    }
  }
}
