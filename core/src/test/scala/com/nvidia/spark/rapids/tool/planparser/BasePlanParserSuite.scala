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

package com.nvidia.spark.rapids.tool.planparser

import com.nvidia.spark.rapids.BaseTestSuite
import com.nvidia.spark.rapids.tool.ToolTestUtils

import org.apache.spark.sql.rapids.tool.qualification.QualificationAppInfo

class BasePlanParserSuite extends BaseTestSuite {

  val profileLogDir: String = ToolTestUtils.getTestResourcePath("spark-events-profiling")
  val qualLogDir: String = ToolTestUtils.getTestResourcePath("spark-events-qualification")

  def assertSizeAndNotSupported(size: Int, execs: Seq[ExecInfo],
      checkDurations: Boolean = true): Unit = {
    for (t <- Seq(execs)) {
      assert(t.size == size, t)
      assert(t.forall(_.speedupFactor == 1), t)
      assert(t.forall(_.isSupported == false), t)
      assert(t.forall(_.children.isEmpty), t)
      if (checkDurations) {
        assert(t.forall(_.duration.isEmpty), t)
      }
    }
  }

  def assertSizeAndSupported(size: Int, execs: Seq[ExecInfo],
      expectedDur: Seq[Option[Long]] = Seq.empty, extraText: String = "",
      checkDurations: Boolean = true): Unit = {
    for (t <- Seq(execs)) {
      assert(t.size == size, s"$extraText $t")
      assert(t.forall(_.isSupported == true), s"$extraText $t")
      assert(t.forall(_.children.isEmpty), s"$extraText $t")
      if (expectedDur.nonEmpty) {
        val durations = t.map(_.duration)
        assert(durations.diff(expectedDur).isEmpty,
          s"$extraText durations differ expected ${expectedDur.mkString(",")} " +
            s"but got ${durations.mkString(",")}")
      } else if (checkDurations) {
        assert(t.forall(_.duration.isEmpty), s"$extraText $t")
      }
    }
  }

  def getAllExecsFromPlan(plans: Seq[PlanInfo]): Seq[ExecInfo] = {
    val topExecInfo = plans.flatMap(_.execInfo)
    topExecInfo.flatMap { e =>
      e.children.getOrElse(Seq.empty) :+ e
    }
  }

  def verifyPlanExecToStageMap(toolsPlanInfo: PlanInfo): Unit = {
    val allExecInfos = toolsPlanInfo.execInfo.flatMap { e =>
      e.children.getOrElse(Seq.empty) :+ e
    }
    // Test that all execs are assigned to stages
    assert (allExecInfos.forall(_.stages.nonEmpty))
    // assert that exchange is assigned to a single stage
    val exchangeExecs = allExecInfos.filter(_.exec == "Exchange")
    if (exchangeExecs.nonEmpty) {
      assert (exchangeExecs.forall(_.stages.size == 1))
    }
  }

  def verifyExecToStageMapping(plans: Seq[PlanInfo],
    qualApp: QualificationAppInfo, funcCB: Option[PlanInfo => Unit] = None): Unit = {
    // Only iterate on plans with that are associated to jobs
    val associatedSqls = qualApp.jobIdToSqlID.values.toSeq
    val filteredPlans = plans.filter(p => associatedSqls.contains(p.sqlID))
    val func = funcCB.getOrElse(verifyPlanExecToStageMap(_))
    filteredPlans.foreach { plan =>
      func(plan)
    }
  }
}
