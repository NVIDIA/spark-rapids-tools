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

import java.io.File

import com.nvidia.spark.rapids.tool.ToolTestUtils.findFilesRecursively
import com.nvidia.spark.rapids.tool.views.qualification.{QualOutputTableDefinition, QualYamlConfigLoader}
import com.nvidia.spark.rapids.tool.views.qualification.QualReportGenConfProvider.TABLES_CONFIG_PATH
import org.scalatest.Matchers._

/**
 * An implementation to check the output json file.
 */
abstract class QToolOutJsonFileChecker extends QToolResultCheckerTrait {
  // TODO: improve the OOP design to use common methods with CSV out file checkers.
  var qTableLabel: String = "clusterInfoJSONReport"
  var expectedLocPath: Option[Seq[String]] = None
  var noFileGenerated: Boolean = false
  var fileVisitors: Seq[(QToolOutJsonFileChecker, File) => Unit] = Seq.empty

  private var _qOutDir: String = ""
  def qOutDir: String = _qOutDir
  def qOutDir_=(value: String): Unit = {
    _qOutDir = value
  }

  def withTableLabel(label: String): QToolOutJsonFileChecker = {
    qTableLabel = label
    this
  }

  def withNoGeneratedFile(): QToolOutJsonFileChecker = {
    noFileGenerated = true
    this
  }

  def withExpectedLoc(expectedPath: String): QToolOutJsonFileChecker = {
    expectedLocPath = Some(expectedLocPath.getOrElse(Seq()) :+ expectedPath)
    this
  }

  lazy val qTableDefn: QualOutputTableDefinition = {
    val tableSelector: QualOutputTableDefinition => Boolean = { defn =>
      defn.label == qTableLabel
    }
    QualYamlConfigLoader.loadConfig(TABLES_CONFIG_PATH)
      .qualTableDefinitions
      .filter(tableSelector).head
  }

  lazy val actualFiles: Seq[File] = {
    findFilesRecursively(new File(qOutDir), qTableDefn.fileName)
  }

  def withContentVisitor(v: (QToolOutJsonFileChecker, File) => Unit): QToolOutJsonFileChecker = {
    fileVisitors :+= v
    this
  }
}

/**
 * The concrete implementation of QToolOutJsonFileChecker.
 * @param description a description of the test behavior.
 */
case class QToolOutJsonFileCheckerImpl(description: String) extends QToolOutJsonFileChecker {
  override def doFire(qTestCtxt: QToolTestCtxt): Unit = {
    qOutDir = qTestCtxt.outputDirectory
    super.doFire(qTestCtxt)
    if (noFileGenerated) {
      actualFiles shouldBe empty
    } else {
      fileVisitors.foreach { fVisitor =>
        fVisitor(this, actualFiles.head)
      }
    }
  }

  override def build(): QToolResultCheckerTrait = this
}
