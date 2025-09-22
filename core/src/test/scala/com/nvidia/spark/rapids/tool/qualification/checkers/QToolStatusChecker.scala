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

package com.nvidia.spark.rapids.tool.qualification.checkers

import com.nvidia.spark.rapids.tool.StatusReportCounts
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

/**
 * Check the status of the qualification result.
 *
 * @param description description of the checker.
 * @param expectedStatusCount the expected counts of the statuses.
 * @param appStatuses the expected statuses for specific apps.
 */
case class QToolStatusChecker(
    description: String,
    var expectedStatusCount: Option[StatusReportCounts] = None,
    var appStatuses: Option[Map[String, Seq[String]]] = None)
  extends QToolResultCheckerTrait {

  private def verifyCounts(): Unit = {
    expectedStatusCount.foreach { expectedCounts =>
      qRes.appStatus.count(s => s.status.equals("SUCCESS")) shouldBe expectedCounts.success
      qRes.appStatus.count(s => s.status.equals("FAILURE")) shouldBe expectedCounts.failure
      qRes.appStatus.count(s => s.status.equals("SKIPPED")) shouldBe expectedCounts.skipped
      qRes.appStatus.count(s => s.status.equals("UNKNOWN")) shouldBe expectedCounts.unknown
    }
  }

  def verifyAppStatus(): Unit = {
    appStatuses.foreach { statusMap =>
      statusMap.foreach { case (status, appIds) =>
        appIds.foreach { appId =>
          val matchingStatuses =
            qRes.appStatus.filter { app =>
              app.appId == appId && app.status == status
            }
          matchingStatuses.nonEmpty shouldBe true
        }
      }
    }
  }

  def withExpectedCounts(expectedCounts: StatusReportCounts): QToolStatusChecker = {
    expectedStatusCount = Some(expectedCounts)
    this
  }

  def withAppStatuses(statusMap: Map[String, Seq[String]]): QToolStatusChecker = {
    appStatuses = Some(statusMap)
    this
  }

  override def doFire(qTestCtxt: QToolTestCtxt): Unit = {
    super.doFire(qTestCtxt)
    verifyCounts()
    verifyAppStatus()
  }

  override def build(): QToolResultCheckerTrait = this
}
