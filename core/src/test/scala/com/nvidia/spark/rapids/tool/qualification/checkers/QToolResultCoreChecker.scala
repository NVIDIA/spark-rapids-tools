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

import com.nvidia.spark.rapids.tool.qualification.QualToolResult
import org.scalatest.Matchers.convertToAnyShouldWrapper

/**
 * Checkers for the core information in the result of the qualification tool.
 * @param description description of the checker.
 * @param expectedSize expected size of the result.
 * @param returnCode expected return code of the tool.
 * @param checkBlocks additional check blocks to add.
 */
case class QToolResultCoreChecker(
    description: String,
    var expectedSize: Option[Int] = None,
    var returnCode: Option[Int] = None,
    var checkBlocks: Seq[QToolResBlockChecker] = Seq.empty) extends QToolResultCheckerTrait{

  def withExpectedSize(size: Int): QToolResultCoreChecker = {
    expectedSize = Some(size)
    this
  }

  def withEmptyResults(): QToolResultCoreChecker = {
    expectedSize = Some(0)
    this
  }

  def withSuccessCode(): QToolResultCoreChecker = {
    returnCode = Some(0)
    this
  }

  def withFailureCode(): QToolResultCoreChecker = {
    returnCode = Some(1)
    this
  }

  def withCheckBlock(
    blockName: String,
    blockFunc: QualToolResult => Unit): QToolResultCoreChecker = {
    checkBlocks = checkBlocks :+ QToolResBlockChecker(blockName, blockFunc)
    this
  }

  def verifyReturnCode(): Unit = {
    returnCode.foreach { code =>
      qRes.returnCode shouldBe code
    }
  }

  def verifySize(): Unit = {
    expectedSize.foreach { size =>
      qRes.appSummaries.size shouldBe size
    }
  }

  def verifyBlocks(): Unit = {
    checkBlocks.foreach { checkObj =>
      checkObj.doFire(qRes)
    }
  }

  override def doFire(qTestCtxt: QToolTestCtxt): Unit = {
    super.doFire(qTestCtxt)
    verifySize()
    verifyReturnCode()
    verifyBlocks()
  }

  override def build(): QToolResultCheckerTrait = this
}
