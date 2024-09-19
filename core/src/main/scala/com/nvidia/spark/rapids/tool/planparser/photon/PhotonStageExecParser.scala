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

import com.nvidia.spark.rapids.tool.planparser.WholeStageExecParserBase
import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker

import org.apache.spark.sql.rapids.tool.AppBase
import org.apache.spark.sql.rapids.tool.util.PhotonSparkPlanGraphCluster

/**
 * Parses Photon Stage Exec equivalent to Spark's `WholeStageCodegenExec`
 * (e.g. PhotonShuffleMapStage, PhotonUnionShuffleMapStage etc.).
 * This can be extended if specific parsing is needed for a particular Stage operator.
 *
 * @see Resource file in `photonOperatorMappings` for the mapping of Photon operators
 *      to Spark operators
 */
case class PhotonStageExecParser(
    node: PhotonSparkPlanGraphCluster,
    checker: PluginTypeChecker,
    sqlID: Long,
    app: AppBase,
    reusedNodeIds: Set[Long])
  extends WholeStageExecParserBase(node, checker, sqlID, app, reusedNodeIds)
