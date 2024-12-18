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

package com.nvidia.spark.rapids.tool.qualification

import scala.collection.mutable

import com.nvidia.spark.rapids.tool.{PlatformFactory, PlatformNames}
import com.nvidia.spark.rapids.tool.profiling.{BaseAutoTunerSuite, Profiler}

/**
 * Suite to test the Qualification Tool's AutoTuner
 */
class QualificationAutoTunerSuite extends BaseAutoTunerSuite {

  /**
   * Helper method to build a worker info string with CPU properties
   */
  protected def buildCpuWorkerInfoAsString(
       customProps: Option[mutable.Map[String, String]] = None,
       numCores: Option[Int] = Some(32),
       systemMemory: Option[String] = Some("122880MiB"),
       numWorkers: Option[Int] = Some(4)): String = {
    buildWorkerInfoAsString(customProps, numCores, systemMemory, numWorkers)
  }

  test("test AutoTuner for Qualification sets batch size to 1GB") {
    // mock the properties loaded from eventLog
    val logEventsProps: mutable.Map[String, String] =
      mutable.LinkedHashMap[String, String](
        "spark.executor.cores" -> "32",
        "spark.executor.instances" -> "1",
        "spark.executor.memory" -> "80g",
        "spark.executor.instances" -> "1"
      )
    val workerInfo = buildCpuWorkerInfoAsString(None, Some(32),
      Some("212992MiB"), Some(5))
    val infoProvider = getMockInfoProvider(0, Seq(0), Seq(0.0),
      logEventsProps,  Some(defaultSparkVersion))
    val clusterPropsOpt = QualificationAutoTunerConfigsProvider
      .loadClusterPropertiesFromContent(workerInfo)
    val platform = PlatformFactory.createInstance(PlatformNames.EMR, clusterPropsOpt)
    val autoTuner = QualificationAutoTunerConfigsProvider.buildAutoTunerFromProps(
      workerInfo, infoProvider, platform)
    val (properties, comments) = autoTuner.getRecommendedProperties()
    val autoTunerOutput = Profiler.getAutoTunerResultsAsString(properties, comments)
    val expectedResults = Seq(
        "--conf spark.rapids.sql.batchSizeBytes=1073741824",
        "- 'spark.rapids.sql.batchSizeBytes' was not set."
    )
    assert(expectedResults.forall(autoTunerOutput.contains))
  }
}
