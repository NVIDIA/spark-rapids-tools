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

package com.nvidia.spark.rapids.tool.tuning

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.nvidia.spark.rapids.tool.AppSummaryInfoBaseProvider
import com.nvidia.spark.rapids.tool.profiling._
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.yaml.snakeyaml.{DumperOptions, Yaml}

import org.apache.spark.internal.Logging


case class DriverInfoProviderMockTest(unsupportedOps: Seq[DriverLogUnsupportedOperators])
  extends BaseDriverLogInfoProvider(None) {
  override def getUnsupportedOperators: Seq[DriverLogUnsupportedOperators] = unsupportedOps
}

class AppInfoProviderMockTest(val maxInput: Double,
    val spilledMetrics: Seq[Long],
    val jvmGCFractions: Seq[Double],
    val propsFromLog: mutable.Map[String, String],
    val sparkVersion: Option[String],
    val rapidsJars: Seq[String],
    val distinctLocationPct: Double,
    val redundantReadSize: Long,
    val meanInput: Double,
    val meanShuffleRead: Double,
    val shuffleStagesWithPosSpilling: Set[Long],
    val shuffleSkewStages: Set[Long]) extends AppSummaryInfoBaseProvider {
  override def isAppInfoAvailable = true
  override def getMaxInput: Double = maxInput
  override def getMeanInput: Double = meanInput
  override def getMeanShuffleRead: Double = meanShuffleRead
  override def getSpilledMetrics: Seq[Long] = spilledMetrics
  override def getJvmGCFractions: Seq[Double] = jvmGCFractions
  override def getRapidsProperty(propKey: String): Option[String] = propsFromLog.get(propKey)
  override def getSparkProperty(propKey: String): Option[String] = propsFromLog.get(propKey)
  override def getSystemProperty(propKey: String): Option[String] = propsFromLog.get(propKey)
  override def getSparkVersion: Option[String] = sparkVersion
  override def getRapidsJars: Seq[String] = rapidsJars
  override def getDistinctLocationPct: Double = distinctLocationPct
  override def getRedundantReadSize: Long = redundantReadSize
  override def getShuffleStagesWithPosSpilling: Set[Long] = shuffleStagesWithPosSpilling
  override def getShuffleSkewStages: Set[Long] = shuffleSkewStages
}

/**
 * Base class for AutoTuner test suites
 */
abstract class BaseAutoTunerSuite extends FunSuite with BeforeAndAfterEach with Logging {

  val defaultSparkVersion = "3.1.1"

  val defaultDataprocProps: mutable.Map[String, String] = {
    mutable.LinkedHashMap[String, String](
      "spark.dynamicAllocation.enabled" -> "true",
      "spark.driver.maxResultSize" -> "7680m",
      "spark.driver.memory" -> "15360m",
      "spark.executor.cores" -> "16",
      "spark.executor.instances" -> "2",
      "spark.executor.resource.gpu.amount" -> "1",
      "spark.executor.memory" -> "26742m",
      "spark.executor.memoryOverhead" -> "7372m",
      "spark.executorEnv.OPENBLAS_NUM_THREADS" -> "1",
      "spark.extraListeners" -> "com.google.cloud.spark.performance.DataprocMetricsListener",
      "spark.rapids.memory.pinnedPool.size" -> "2048m",
      "spark.scheduler.mode" -> "FAIR",
      "spark.sql.cbo.enabled" -> "true",
      "spark.sql.adaptive.enabled" -> "true",
      "spark.ui.port" -> "0",
      "spark.yarn.am.memory" -> "640m"
    )
  }

  protected final def buildWorkerInfoAsString(
      customProps: Option[mutable.Map[String, String]] = None,
      numCores: Option[Int] = Some(32),
      systemMemory: Option[String] = Some("122880MiB"),
      numWorkers: Option[Int] = Some(4),
      gpuCount: Option[Int] = None,
      gpuMemory: Option[String] = None,
      gpuDevice: Option[String] = None): String = {
    val gpuWorkerProps = new GpuWorkerProps(
      gpuMemory.getOrElse(""), gpuCount.getOrElse(0), gpuDevice.getOrElse(""))
    val cpuSystem = new SystemClusterProps(
      numCores.getOrElse(0), systemMemory.getOrElse(""), numWorkers.getOrElse(0))
    val systemProperties = customProps match {
      case None => mutable.Map[String, String]()
      case Some(newProps) => newProps
    }
    val convertedMap = new util.LinkedHashMap[String, String](systemProperties.asJava)
    val clusterProps = new ClusterProperties(cpuSystem, gpuWorkerProps, convertedMap)
    // set the options to convert the object into formatted yaml content
    val options = new DumperOptions()
    options.setIndent(2)
    options.setPrettyFlow(true)
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
    val yaml = new Yaml(options)
    val rawString = yaml.dump(clusterProps)
    // Skip the first line as it contains "the name of the class"
    rawString.split("\n").drop(1).mkString("\n")
  }

  protected def getMockInfoProvider(maxInput: Double,
      spilledMetrics: Seq[Long],
      jvmGCFractions: Seq[Double],
      propsFromLog: mutable.Map[String, String],
      sparkVersion: Option[String],
      rapidsJars: Seq[String] = Seq(),
      distinctLocationPct: Double = 0.0,
      redundantReadSize: Long = 0,
      meanInput: Double = 0.0,
      meanShuffleRead: Double = 0.0,
      shuffleStagesWithPosSpilling: Set[Long] = Set(),
      shuffleSkewStages: Set[Long] = Set()): AppSummaryInfoBaseProvider = {
    new AppInfoProviderMockTest(maxInput, spilledMetrics, jvmGCFractions, propsFromLog,
      sparkVersion, rapidsJars, distinctLocationPct, redundantReadSize, meanInput, meanShuffleRead,
      shuffleStagesWithPosSpilling, shuffleSkewStages)
  }
}
