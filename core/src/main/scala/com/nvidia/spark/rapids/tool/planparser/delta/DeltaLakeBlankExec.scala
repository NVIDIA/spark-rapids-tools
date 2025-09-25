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

package com.nvidia.spark.rapids.tool.planparser.delta

import com.nvidia.spark.rapids.tool.planparser.{OpTypes, SupportedBlankExec, SupportedOpStub}
import com.nvidia.spark.rapids.tool.qualification.PluginTypeChecker

import org.apache.spark.sql.execution.ui.SparkPlanGraphNode
import org.apache.spark.sql.rapids.tool.AppBase

/**
 * Create an execInfo for a Delta Lake operator. Some Delta Lake operators do not have content in
 * their node description. Therefore, we override the GenericParser to create a SupportedBlankExec
 * for these operators. The execName, supportedFlag, and opType are provided by the
 * SupportedOpStub.
 * @param node the SparkPlanGraphNode
 * @param checker plugin type checker
 * @param sqlID the SQL ID
 * @param opStub the SupportedOpStub containing static information about the operator
 * @param app the AppBase instance.
 */
class DeltaLakeBlankExec(
    override val node: SparkPlanGraphNode,
    override val checker: PluginTypeChecker,
    override val sqlID: Long,
    opStub: SupportedOpStub,
    override val app: Option[AppBase]
) extends SupportedBlankExec (
    node = node,
    checker = checker,
    sqlID = sqlID,
    app = app) {

  override def getDurationSqlMetrics: Set[String] = {
    opStub.sqlMetricNames
  }

  override lazy val fullExecName: String = opStub.execID

  override def reportedExecName: String = opStub.nodeName

  override def pullSupportedFlag(registeredName: Option[String] = None): Boolean = {
    opStub.isSupported
  }

  override def pullOpType: OpTypes.Value = {
    opStub.pullOpType
  }
}
