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

package com.nvidia.spark.rapids.tool.profiling

import java.io.File

import com.nvidia.spark.rapids.tool.{GpuTypes, PlatformNames, StatusReportCounts, ToolTestUtils}
import com.nvidia.spark.rapids.tool.views.CLUSTER_INFORMATION_LABEL
import org.scalatest.FunSuite
import org.scalatest.exceptions.TestFailedException
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor3, TableFor4}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, TrampolineUtil}
import org.apache.spark.sql.rapids.tool.RecommendedClusterInfo


class ClusterRecommendationSuite extends FunSuite with Logging with TableDrivenPropertyChecks {
  lazy val sparkSession: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("Rapids Spark Profiling Tool Unit Tests")
      .getOrCreate()
  }

  private val logDir = ToolTestUtils.getTestResourcePath("spark-events-profiling")

  val validClusterRecommendationScenarios: TableFor4[
      String, String, Option[Int], RecommendedClusterInfo] = Table(
    // Define the column headers
    ("platform", "instanceType", "gpuCount", "expectedClusterInfo"),
    // Test scenarios
    (
      PlatformNames.DATAPROC,
      "n1-standard-16",
      Some(1),
      RecommendedClusterInfo(
        vendor = PlatformNames.DATAPROC,
        coresPerExecutor = 16,
        numWorkerNodes = 8,
        numGpusPerNode = 1,
        numExecutors = 8,
        gpuDevice = GpuTypes.T4,
        dynamicAllocationEnabled = false,
        dynamicAllocationMaxExecutors = "N/A",
        dynamicAllocationMinExecutors = "N/A",
        dynamicAllocationInitialExecutors = "N/A",
        workerNodeType = Some("n1-standard-16"))
    ),
    (
      PlatformNames.DATAPROC,
      "n1-standard-32",
      Some(2),
      RecommendedClusterInfo(
        vendor = PlatformNames.DATAPROC,
        coresPerExecutor = 16,
        numWorkerNodes = 4,
        numGpusPerNode = 2,
        numExecutors = 8,
        gpuDevice = GpuTypes.T4,
        dynamicAllocationEnabled = false,
        dynamicAllocationMaxExecutors = "N/A",
        dynamicAllocationMinExecutors = "N/A",
        dynamicAllocationInitialExecutors = "N/A",
        workerNodeType = Some("n1-standard-32"))
    ),
    (
      PlatformNames.DATAPROC,
      "g2-standard-16",
      None,
      RecommendedClusterInfo(
        vendor = PlatformNames.DATAPROC,
        coresPerExecutor = 16,
        numWorkerNodes = 8,
        numGpusPerNode = 1,
        numExecutors = 8,
        gpuDevice = GpuTypes.L4,
        dynamicAllocationEnabled = false,
        dynamicAllocationMaxExecutors = "N/A",
        dynamicAllocationMinExecutors = "N/A",
        dynamicAllocationInitialExecutors = "N/A",
        workerNodeType = Some("g2-standard-16"))
    ),
    (
      PlatformNames.EMR,
      "g6.8xlarge",
      None,
      RecommendedClusterInfo(
        vendor = PlatformNames.EMR,
        coresPerExecutor = 32,
        numWorkerNodes = 8,
        numGpusPerNode = 1,
        numExecutors = 8,
        gpuDevice = GpuTypes.L4,
        dynamicAllocationEnabled = false,
        dynamicAllocationMaxExecutors = "N/A",
        dynamicAllocationMinExecutors = "N/A",
        dynamicAllocationInitialExecutors = "N/A",
        workerNodeType = Some("g6.8xlarge"))
    ),
    (
      PlatformNames.DATABRICKS_AWS,
      "g5.8xlarge",
      None,
      RecommendedClusterInfo(
        vendor = PlatformNames.DATABRICKS_AWS,
        coresPerExecutor = 32,
        numWorkerNodes = 8,
        numGpusPerNode = 1,
        numExecutors = 8,
        gpuDevice = GpuTypes.A10G,
        dynamicAllocationEnabled = false,
        dynamicAllocationMaxExecutors = "N/A",
        dynamicAllocationMinExecutors = "N/A",
        dynamicAllocationInitialExecutors = "N/A",
        workerNodeType = Some("g5.8xlarge"))
    )
  )
  // scalastyle:on line.size.limit
  forAll(validClusterRecommendationScenarios) {
    (platform, instanceType, gpuCount, expectedClusterInfo) =>
      val scenarioName = gpuCount match {
        case Some(count) => s"$instanceType with $count GPUs"
        case None => instanceType
      }
      test(s"test valid cluster shape recommendation on $platform - $scenarioName") {
        TrampolineUtil.withTempDir { tempDir =>
          val targetClusterInfoFile = ToolTestUtils.createTargetClusterInfoFile(
            tempDir.getAbsolutePath,
            instanceType = instanceType,
            gpuCount = gpuCount)

          val appArgs = new ProfileArgs(Array(
            "--platform",
            platform,
            "--target-cluster-info",
            targetClusterInfoFile.toString,
            "--output-directory",
            tempDir.getAbsolutePath,
            "--csv",
            "--auto-tuner",
            s"$logDir/nds_q66_gpu.zstd"))

          val (exit, _) = ProfileMain.mainInternal(appArgs)
          assert(exit == 0)
          val tempSubDir = new File(tempDir, s"${Profiler.SUBDIR}/application_1701368813061_0008")
          val fileName = CLUSTER_INFORMATION_LABEL.replace(" ", "_").toLowerCase
          val actualFilePath = s"${tempSubDir.getAbsolutePath}/$fileName.json"

          val recommendedClusterInfo = ToolTestUtils.loadClusterSummaryFromJson(actualFilePath)
            .headOption.flatMap(_.recommendedClusterInfo).getOrElse {
              throw new TestFailedException(
                s"Failed to load recommended cluster info from $actualFilePath", 0)
            }

          val clusterInfoMatches = recommendedClusterInfo == expectedClusterInfo
          assert(clusterInfoMatches,
            s"""
               |Actual cluster info does not match the expected cluster info.
               |Actual: $recommendedClusterInfo
               |Expected: $expectedClusterInfo
               |""".stripMargin)
        }
      }
  }

  test(s"test valid cluster shape recommendation on onprem") {
    val expectedClusterInfo = RecommendedClusterInfo(
      vendor = PlatformNames.ONPREM,
      coresPerExecutor = 16,
      numWorkerNodes = -1,
      numGpusPerNode = 1,
      numExecutors = 8,
      gpuDevice = GpuTypes.L4,
      dynamicAllocationEnabled = false,
      dynamicAllocationMaxExecutors = "N/A",
      dynamicAllocationMinExecutors = "N/A",
      dynamicAllocationInitialExecutors = "N/A",
      workerNodeType = Some("N/A"))
    TrampolineUtil.withTempDir { tempDir =>
      val appArgs = new ProfileArgs(Array(
        "--platform",
        PlatformNames.ONPREM,
        "--output-directory",
        tempDir.getAbsolutePath,
        "--csv",
        "--auto-tuner",
        s"$logDir/nds_q66_gpu.zstd"))

      val (exit, _) = ProfileMain.mainInternal(appArgs)
      assert(exit == 0)
      val tempSubDir = new File(tempDir, s"${Profiler.SUBDIR}/application_1701368813061_0008")
      val fileName = CLUSTER_INFORMATION_LABEL.replace(" ", "_").toLowerCase
      val actualFilePath = s"${tempSubDir.getAbsolutePath}/$fileName.json"

      val recommendedClusterInfo = ToolTestUtils.loadClusterSummaryFromJson(actualFilePath)
        .headOption.flatMap(_.recommendedClusterInfo).getOrElse {
          throw new TestFailedException(
            s"Failed to load recommended cluster info from $actualFilePath", 0)
        }

      val clusterInfoMatches = recommendedClusterInfo == expectedClusterInfo
      assert(clusterInfoMatches,
        s"""
           |Actual cluster info does not match the expected cluster info.
           |Actual: $recommendedClusterInfo
           |Expected: $expectedClusterInfo
           |""".stripMargin)
    }
  }

  val invalidClusterInfoScenarios: TableFor3[String, String, Option[Int]] = Table(
    // Define the column headers
    ("platform", "instanceType", "gpuCount"),
    // Test scenarios
    (PlatformNames.DATAPROC, "n1-standard-4", None),
    (PlatformNames.EMR, "abc.8xlarge", None),
    (PlatformNames.DATABRICKS_AWS, "g5.2xlarge", Some(4))
  )

  forAll(invalidClusterInfoScenarios) {
    (platform, instanceType, gpuCount) =>
      val scenarioName = gpuCount match {
        case Some(count) => s"$instanceType with $count GPUs"
        case None => instanceType
      }
      test(s"test invalid cluster shape recommendation on $platform - $scenarioName") {
        TrampolineUtil.withTempDir { tempDir =>
          val targetClusterInfoFile = ToolTestUtils.createTargetClusterInfoFile(
            tempDir.getAbsolutePath,
            instanceType = instanceType,
            gpuCount = gpuCount)

          val appArgs = new ProfileArgs(Array(
            "--platform",
            platform,
            "--target-cluster-info",
            targetClusterInfoFile.toString,
            "--output-directory",
            tempDir.getAbsolutePath,
            "--csv",
            "--auto-tuner",
            s"$logDir/nds_q66_gpu.zstd"))

          val (exit, _) = ProfileMain.mainInternal(appArgs)
          assert(exit == 0)
          // Status counts: 0 SUCCESS, 0 FAILURE, 0 SKIPPED, 1 UNKNOWN
          val expectedStatusCount = StatusReportCounts(0, 0, 0, 1)
          ToolTestUtils.compareStatusReport(sparkSession, expectedStatusCount,
            s"${tempDir.getAbsolutePath}/${Profiler.SUBDIR}/profiling_status.csv")
        }
      }
  }
}
