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

package com.nvidia.spark.rapids.tool.planparser.photon

import com.nvidia.spark.rapids.tool.planparser._
import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker

import org.apache.spark.sql.rapids.tool.AppBase
import org.apache.spark.sql.rapids.tool.util.PhotonSparkPlanGraphNode

object PhotonPlanParser {
  /**
   * Parses a Photon node, using a specific Photon parser if available.
   * If no Photon specific parser is defined, use Spark CPU equivalent
   * ExecParser.
   *
   * @return Parsed ExecInfo
   */
  def parseNode(
      node: PhotonSparkPlanGraphNode,
      sqlID: Long,
      checker: PluginTypeChecker,
      app: AppBase): ExecInfo = {
    node.photonName match {
      case "PhotonBroadcastNestedLoopJoin" =>
        PhotonBroadcastNestedLoopJoinExecParser(node, checker, sqlID).parse
      case _ =>
        // If no photon specific parser is defined, parse using Spark CPU parser.
        // This is allowed because `node.name` and `node.desc` are set to the Spark CPU
        // operator's name and description.
        SQLPlanParser.parseSparkNode(node, sqlID, checker, app)
    }
  }
}
