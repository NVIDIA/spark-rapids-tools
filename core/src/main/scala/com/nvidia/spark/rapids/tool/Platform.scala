/*
 * Copyright (c) 2023-2025, NVIDIA CORPORATION.
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
package com.nvidia.spark.rapids.tool

import scala.annotation.tailrec

import com.nvidia.spark.rapids.tool.planparser.DatabricksParseHelper
import com.nvidia.spark.rapids.tool.tuning.{ClusterProperties, ProfilingAutoTunerConfigsProvider, SparkMaster}

import org.apache.spark.internal.Logging
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.rapids.tool.{ExistingClusterInfo, RecommendedClusterInfo}
import org.apache.spark.sql.rapids.tool.util.{SparkRuntime, StringUtils}

/**
 *  Utility object containing constants for various platform names.
 */
object PlatformNames {
  val DATABRICKS_AWS = "databricks-aws"
  val DATABRICKS_AZURE = "databricks-azure"
  val DATAPROC = "dataproc"
  val DATAPROC_GKE = "dataproc-gke"
  val DATAPROC_SL = "dataproc-serverless"
  val EMR = "emr"
  val ONPREM = "onprem"
  val DEFAULT: String = ONPREM

  /**
   * Return a list of all supported platform names.
   */
  def getAllNames: List[String] = List(
    DATABRICKS_AWS, s"$DATABRICKS_AWS-$A10GGpu", s"$DATABRICKS_AWS-$T4Gpu",
    DATABRICKS_AZURE, s"$DATABRICKS_AZURE-$T4Gpu",
    DATAPROC, s"$DATAPROC-$L4Gpu", s"$DATAPROC-$T4Gpu",
    DATAPROC_GKE, s"$DATAPROC_GKE-$L4Gpu", s"$DATAPROC_GKE-$T4Gpu",
    DATAPROC_SL, s"$DATAPROC_SL-$L4Gpu",
    EMR, s"$EMR-$A10Gpu", s"$EMR-$A10GGpu", s"$EMR-$T4Gpu",
    ONPREM, s"$ONPREM-$A100Gpu"
  )
}

case class DynamicAllocationInfo(enabled: Boolean, max: String, min: String, initial: String)

// resource information and name of the CSP instance types, or for onprem its
// the executor information since we can't recommend node types
case class InstanceInfo(cores: Int, memoryMB: Long, name: String, numGpus: Int) {
  /**
   * Get the memory per executor based on the instance type.
   * Note: For GPU instances, num of executors is same as the number of GPUs.
   */
  def getMemoryPerExec: Double = {
    memoryMB.toDouble / numGpus
  }
}

object InstanceInfo {
    def createDefaultInstance(cores: Int, memoryMB: Long, numGpus: Int): InstanceInfo = {
      InstanceInfo(cores, memoryMB, "N/A", numGpus)
    }
}

// This is meant to be temporary mapping to figure out instance type based
// on the number of GPUs and cores.  Eventually this should be read from files
// generated based on CSP instance information.
// format (numGpus, numCores) -> InstanceInfo about that CSP node instance type
object PlatformInstanceTypes {

  // Using G6 instances for EMR
  val EMR_BY_GPUS_CORES = Map((1, 4) -> InstanceInfo(4, 16 * 1024, "g6.xlarge", 1),
    (1, 8) -> InstanceInfo(8, 32 * 1024, "g6.2xlarge", 1),
    (1, 16) -> InstanceInfo(16, 64 * 1024, "g6.4xlarge", 1),
    (1, 32) -> InstanceInfo(32, 128 * 1024, "g6.8xlarge", 1),
    (4, 48) -> InstanceInfo(48, 192 * 1024, "g6.12xlarge", 1),
    (1, 64) -> InstanceInfo(64, 256 * 1024, "g6.16xlarge", 1)
  )

  // Using G5 instances. To be updated once G6 availability on Databricks
  // is consistent
  val DATABRICKS_AWS_BY_GPUS_CORES = Map((1, 4) -> InstanceInfo(4, 16 * 1024, "g5.xlarge", 1),
    (1, 8) -> InstanceInfo(8, 32 * 1024, "g5.2xlarge", 1),
    (1, 16) -> InstanceInfo(16, 64 * 1024, "g5.4xlarge", 1),
    (1, 32) -> InstanceInfo(32, 128 * 1024, "g5.8xlarge", 1),
    (4, 48) -> InstanceInfo(48, 192 * 1024, "g5.12xlarge", 1),
    (1, 64) -> InstanceInfo(64, 256 * 1024, "g5.16xlarge", 1)
  )

  // Standard_NC4as_T4_v3 - only recommending nodes with T4's for now, add more later
  val AZURE_NCAS_T4_V3_BY_GPUS_CORES = Map(
    (1, 4) -> InstanceInfo(4, 28 * 1024, "Standard_NC4as_T4_v3", 1), // 1 GPU
    (1, 8) -> InstanceInfo(8, 56 * 1024, "Standard_NC8as_T4_v3", 1), // 1 GPU
    (1, 16) -> InstanceInfo(16, 110 * 1024, "Standard_NC16as_T4_v3", 1), // 1 GPU
    (4, 64) -> InstanceInfo(64, 440 * 1024, "Standard_NC64as_T4_v3", 4)  // 4 GPUs
  )

  // dataproc and dataproc-gke
  // Google supports 1, 2, or 4 Gpus of most types
  // added to n1-standard boxes. You may be able to add 8 v100's but we
  // are going to ignore that.
  val DATAPROC_BY_GPUS_CORES = Map(
    (1, 1) -> InstanceInfo(1, 1 * 3840, "n1-standard-1", 1),
    (1, 2) -> InstanceInfo(2, 2 * 3840, "n1-standard-2", 1),
    (1, 4) -> InstanceInfo(4, 4 * 3840, "n1-standard-4", 1),
    (1, 8) -> InstanceInfo(8, 8 * 3840, "n1-standard-8", 1),
    (1, 16) -> InstanceInfo(16, 16 * 3840, "n1-standard-16", 1),
    (1, 32) -> InstanceInfo(32, 32 * 3840, "n1-standard-32", 1),
    (1, 64) -> InstanceInfo(64, 64 * 3840, "n1-standard-64", 1),
    (1, 96) -> InstanceInfo(96, 96 * 3840, "n1-standard-96", 1),
    (2, 1) -> InstanceInfo(1, 1 * 3840, "n1-standard-1", 2),
    (2, 2) -> InstanceInfo(2, 2 * 3840, "n1-standard-2", 2),
    (2, 4) -> InstanceInfo(4, 4 * 3840, "n1-standard-4", 2),
    (2, 8) -> InstanceInfo(8, 8 * 3840, "n1-standard-8", 2),
    (2, 16) -> InstanceInfo(16, 16 * 3840, "n1-standard-16", 2),
    (2, 32) -> InstanceInfo(32, 32 * 3840, "n1-standard-32", 2),
    (2, 64) -> InstanceInfo(64, 64 * 3840, "n1-standard-64", 2),
    (2, 96) -> InstanceInfo(96, 96 * 3840, "n1-standard-96", 2),
    (4, 1) -> InstanceInfo(1, 1 * 3840, "n1-standard-1", 4),
    (4, 2) -> InstanceInfo(2, 2 * 3840, "n1-standard-2", 4),
    (4, 4) -> InstanceInfo(4, 4 * 3840, "n1-standard-4", 4),
    (4, 8) -> InstanceInfo(8, 8 * 3840, "n1-standard-8", 4),
    (4, 16) -> InstanceInfo(16, 16 * 3840, "n1-standard-16", 4),
    (4, 32) -> InstanceInfo(32, 32 * 3840, "n1-standard-32", 4),
    (4, 64) -> InstanceInfo(64, 64 * 3840, "n1-standard-64", 4),
    (4, 96) -> InstanceInfo(96, 96 * 3840, "n1-standard-96", 4)
  )
}

/**
 * Represents a platform and its associated recommendations.
 *
 * @param gpuDevice Gpu Device present in the platform
 * @param clusterProperties Cluster Properties passed into the tool as worker info
 */
abstract class Platform(var gpuDevice: Option[GpuDevice],
    val clusterProperties: Option[ClusterProperties]) extends Logging {
  val platformName: String
  val defaultGpuDevice: GpuDevice
  val sparkVersionLabel: String = "Spark version"

  // It's not deal to use vars here but to minimize changes and
  // keep backwards compatibility we put them here for now and hopefully
  // in future we can refactor.
  var clusterInfoFromEventLog: Option[ExistingClusterInfo] = None
  // instance information for the gpu node type we will use to run with
  var recommendedNodeInstanceInfo: Option[InstanceInfo] = None
  // overall final recommended cluster configuration
  var recommendedClusterInfo: Option[RecommendedClusterInfo] = None

  // Default recommendation based on NDS benchmarks (note: this could be platform specific)
  def recommendedCoresPerExec: Int = ProfilingAutoTunerConfigsProvider.DEF_CORES_PER_EXECUTOR
  // Default number of GPUs to use, currently we do not support multiple GPUs per node
  def recommendedGpusPerNode = 1
  def defaultNumGpus: Int = 1

  // Default runtime for the platform
  val defaultRuntime: SparkRuntime.SparkRuntime = SparkRuntime.SPARK
  // Set of supported runtimes for the platform
  protected val supportedRuntimes: Set[SparkRuntime.SparkRuntime] = Set(
    SparkRuntime.SPARK, SparkRuntime.SPARK_RAPIDS
  )

  // scalastyle:off line.size.limit
  // Supported Spark version to RapidsShuffleManager version mapping.
  // Reference: https://docs.nvidia.com/spark-rapids/user-guide/latest/additional-functionality/rapids-shuffle.html#rapids-shuffle-manager
  // scalastyle:on line.size.limit
  val supportedShuffleManagerVersionMap: Array[(String, String)] = Array(
    "3.2.0" -> "320",
    "3.2.1" -> "321",
    "3.2.2" -> "322",
    "3.2.3" -> "323",
    "3.2.4" -> "324",
    "3.3.0" -> "330",
    "3.3.1" -> "331",
    "3.3.2" -> "332",
    "3.3.3" -> "333",
    "3.3.4" -> "334",
    "3.4.0" -> "340",
    "3.4.1" -> "341",
    "3.4.2" -> "342",
    "3.4.3" -> "343",
    "3.5.0" -> "350",
    "3.5.1" -> "351"
  )

  /**
   * Determine the appropriate RapidsShuffleManager version based on the
   * provided spark version.
   */
  def getShuffleManagerVersion(sparkVersion: String): Option[String] = {
    supportedShuffleManagerVersionMap.collectFirst {
      case (supportedVersion, smVersion) if sparkVersion.contains(supportedVersion) => smVersion
    }
  }

  /**
   * Identify the latest supported Spark and RapidsShuffleManager version for the platform.
   */
  lazy val latestSupportedShuffleManagerInfo: (String, String) = {
    supportedShuffleManagerVersionMap.maxBy(_._1)
  }

  /**
   * Checks if the given runtime is supported by the platform.
   */
  def isRuntimeSupported(runtime: SparkRuntime.SparkRuntime): Boolean = {
    supportedRuntimes.contains(runtime)
  }

  // This function allow us to have one gpu type used by the auto
  // tuner recommendations but have a different GPU used for speedup
  // factors since we don't have speedup factors for all combinations of
  // platforms and GPUs. We expect speedup factor usage to be going away
  // so this is less of an issue.
  def defaultGpuForSpeedupFactor: GpuDevice = defaultGpuDevice

  /**
   * Recommendations to be excluded from the list of recommendations.
   * These have the highest priority.
   */
  val recommendationsToExclude: Set[String] = Set.empty
  /**
   * Recommendations to be included in the final list of recommendations.
   * These properties should be specific to the platform and not general Spark properties.
   * For example: we used to set "spark.databricks.optimizer.dynamicFilePruning" to false for the
   *              Databricks platform.
   *
   * Represented as a tuple of (propertyKey, propertyValue).
   */
  val recommendationsToInclude: Seq[(String, String)] = Seq.empty
  /**
   * Dynamically calculates the recommendation for a specific Spark property by invoking
   * the appropriate function based on `sparkProperty`.
   * TODO: Implement this function and integrate with existing code in AutoTuner
   *
   * @param sparkProperty The Spark property for which the recommendation is calculated.
   * @param args Variable list of arguments passed to the calculation function for dynamic
   *             processing.
   * @return Optional string containing the recommendation, or `None` if unavailable.
   */
  def getRecommendation(sparkProperty: String, args: Any*): Option[String] = None

  /**
   * Checks if the `property` is valid:
   * 1. It should not be in exclusion list
   *   OR
   * 2. It should be in the inclusion list
   */
  def isValidRecommendation(property: String): Boolean = {
    !recommendationsToExclude.contains(property) ||
      recommendationsToInclude.map(_._1).contains(property)
  }

  /**
   * Checks if the `comment` is valid:
   * 1. It should not have any property from the exclusion list
   */
  def isValidComment(comment: String): Boolean = {
    recommendationsToExclude.forall(excluded => !comment.contains(excluded))
  }

  def getOperatorScoreFile: String = {
    s"operatorsScore-$platformName-$getGpuOrDefaultForSpeedupFactors.csv"
  }

  def getDefaultOperatorScoreFile: String = {
    s"operatorsScore-$platformName-$defaultGpuForSpeedupFactor.csv"
  }

  final def getGpuOrDefault: GpuDevice = gpuDevice.getOrElse(defaultGpuDevice)

  final def getGpuOrDefaultForSpeedupFactors: GpuDevice =
    gpuDevice.getOrElse(defaultGpuForSpeedupFactor)

  final def setGpuDevice(gpuDevice: GpuDevice): Unit = {
    this.gpuDevice = Some(gpuDevice)
  }

  /**
   * Important system properties that should be retained based on platform.
   */
  def getRetainedSystemProps: Set[String] = Set.empty

  def getExecutorHeapMemoryMB(sparkProperties: Map[String, String]): Long = {
    val executorMemoryFromConf = sparkProperties.get("spark.executor.memory")
    if (executorMemoryFromConf.isDefined) {
      StringUtils.convertToMB(executorMemoryFromConf.getOrElse("0"), Some(ByteUnit.BYTE))
    } else {
      // TODO: Potentially enhance this to handle if no config then check the executor
      //  added or resource profile added events for the heap size
      val sparkMaster = SparkMaster(sparkProperties.get("spark.master"))
      sparkMaster.map(_.defaultExecutorMemoryMB).getOrElse(0L)
    }
  }

  def getExecutorOverheadMemoryMB(sparkProperties: Map[String, String]): Long = {
    val executorMemoryOverheadFromConf = sparkProperties.get("spark.executor.memoryOverhead")
    val execMemOverheadFactorFromConf = sparkProperties.get("spark.executor.memoryOverheadFactor")
    val execHeapMemoryMB = getExecutorHeapMemoryMB(sparkProperties)
    if (executorMemoryOverheadFromConf.isDefined) {
      StringUtils.convertToMB(executorMemoryOverheadFromConf.get, Some(ByteUnit.MiB))
    } else if (execHeapMemoryMB > 0) {
      if (execMemOverheadFactorFromConf.isDefined) {
        (execHeapMemoryMB * execMemOverheadFactorFromConf.get.toDouble).toLong
      } else {
        val sparkMasterConf = sparkProperties.get("spark.master")
        sparkMasterConf match {
          case None => 0L
          case Some(sparkMaster) =>
            if (sparkMaster.contains("yarn")) {
              // default is 10%
              (execHeapMemoryMB * 0.1).toLong
            } else if (sparkMaster.contains("k8s")) {
              val k8sOverheadFactor = sparkProperties.get("spark.kubernetes.memoryOverheadFactor")
              if (k8sOverheadFactor.isDefined) {
                (execHeapMemoryMB * k8sOverheadFactor.get.toDouble).toLong
              } else {
                // For JVM-based jobs this value will default to 0.10 and 0.40 for non-JVM jobs
                // TODO - We can't tell above by any property... do we try to parse submit cli?
                (execHeapMemoryMB * 0.1).toLong
              }
            } else if (sparkMaster.startsWith("spark:")) {
              // would be the entire node memory by default, we don't know and user doesn't
              // need to specify
              0L
            } else {
              0L
            }
        }
      }
    } else {
      0L
    }
  }

  def createClusterInfo(coresPerExecutor: Int,
      numExecsPerNode: Int,
      numExecs: Int,
      numWorkerNodes: Int,
      sparkProperties: Map[String, String],
      systemProperties: Map[String, String]): ExistingClusterInfo = {
    val driverHost = sparkProperties.get("spark.driver.host")
    val executorHeapMem = getExecutorHeapMemoryMB(sparkProperties)
    val dynamicAllocSettings = Platform.getDynamicAllocationSettings(sparkProperties)
    ExistingClusterInfo(platformName, coresPerExecutor, numExecsPerNode, numExecs, numWorkerNodes,
      executorHeapMem, dynamicAllocSettings.enabled, dynamicAllocSettings.max,
      dynamicAllocSettings.min, dynamicAllocSettings.initial, driverHost = driverHost)
  }

  // set the cluster information for this platform based on what we found in the
  // eventlog
  def configureClusterInfoFromEventLog(coresPerExecutor: Int,
      execsPerNode: Int,
      numExecs: Int,
      numExecutorNodes: Int,
      sparkProperties: Map[String, String],
      systemProperties: Map[String, String]): Unit = {
    clusterInfoFromEventLog = Some(createClusterInfo(coresPerExecutor, execsPerNode,
      numExecs, numExecutorNodes, sparkProperties, systemProperties))
  }

  override def toString: String = {
    val gpuStr = gpuDevice.fold("")(gpu => s"-$gpu")
    s"$platformName$gpuStr"
  }

  /**
   * Indicate if the platform is a cloud service provider.
   */
  def isPlatformCSP: Boolean = false

  /**
   * Indicate if the platform requires path recommendations
   */
  def requirePathRecommendations: Boolean = true

  /**
   * The maximum number of Gpus any instance in this platform supports.
   */
  def maxGpusSupported: Int = 1

  /**
   * Get the mapping of number of GPUs and cores to the instance type.
   * Format (numGpus, numCores) -> InstanceInfo about the matching CSP node instance type.
   */
  def getInstanceByResourcesMap: Map[(Int, Int), InstanceInfo] = Map.empty

  /**
   * Get the instance type based on the core and gpu requirements.
   * Case A: Find the exact instance type based on the required resources.
   * Case B: If the exact instance type is not found, find the instance type with the smallest
   *         combined GPU and core count that meets the requirements.
   * Case C: For onprem or cases where a matching CSP instance type is unavailable.
   */
  private def getInstanceByResources(
      recommendedClusterConfig: RecommendedClusterConfig): InstanceInfo = {
    val requiredGpus = recommendedClusterConfig.numGpusPerNode
    val requiredCores = recommendedClusterConfig.coresPerNode

    // A. Find the exact instance type based on the required resources
    getInstanceByResourcesMap.get((requiredGpus, requiredCores)).orElse {
      // B. If the exact instance type is not found, find the instance type with the smallest
      //    combined GPU and core count that meets the requirements
      val suitableInstances = getInstanceByResourcesMap.filterKeys { case (gpus, cores) =>
        gpus >= requiredGpus && cores >= requiredCores
      }
      if (suitableInstances.isEmpty) {
        None
      } else {
        val optimalNumGpusAndCoresPair = suitableInstances.keys.minBy { case (gpus, cores) =>
          gpus + cores
        }
        suitableInstances.get(optimalNumGpusAndCoresPair)
      }
    }.getOrElse {
      // C. For onprem or cases where a matching CSP instance type is unavailable
      logDebug(s"Could not find a matching instance with requiredGpus=$requiredGpus," +
        s" requiredCores=$requiredCores. Falling back to create a default instance info: \n" +
        s"coresPerExec=${recommendedClusterConfig.coresPerExec}, " +
        s"memoryPerNodeMb=${recommendedClusterConfig.memoryPerNodeMb}, " +
        s"numGpusPerNode=${recommendedClusterConfig.numGpusPerNode}")
      InstanceInfo.createDefaultInstance(
        cores = recommendedClusterConfig.coresPerExec,
        memoryMB = recommendedClusterConfig.memoryPerNodeMb,
        numGpus = recommendedClusterConfig.numGpusPerNode)
    }
  }

  /**
   * Creates the recommended GPU cluster info based on Spark properties and
   * cluster properties (either provided explicitly by the user or
   * inferred from the event log).
   *
   * @param sparkProperties A map of Spark properties (combined from application and
   *                        cluster properties)
   * @return Optional `RecommendedClusterInfo` containing the GPU cluster configuration
   *         recommendation.
   */
  def createRecommendedGpuClusterInfo(sparkProperties: Map[String, String]): Unit = {
    // Get the appropriate cluster configuration strategy (either
    // 'ClusterPropertyBasedStrategy' based on cluster properties or
    // 'EventLogBasedStrategy' based on the event log).
    val configurationStrategyOpt = ClusterConfigurationStrategy.getStrategy(
      platform = this, sparkProperties = sparkProperties)

    configurationStrategyOpt match {
      case Some(strategy) =>
        // Using the strategy, generate the recommended cluster configuration (num executors,
        // cores per executor, num executors per node).
        strategy.getRecommendedConfig match {
          case Some(clusterConfig) =>
            val recommendedNodeInstance = getInstanceByResources(clusterConfig)
            val vendor = clusterInfoFromEventLog.map(_.vendor).getOrElse("")
            val numWorkerNodes = if (!isPlatformCSP) {
              // For on-prem, we do not have the concept of worker nodes.
              -1
            } else {
              // Calculate number of worker nodes based on executors and GPUs per instance
              math.ceil(clusterConfig.numExecutors.toDouble /
                recommendedNodeInstance.numGpus).toInt
            }

            val dynamicAllocSettings = Platform.getDynamicAllocationSettings(sparkProperties)
            recommendedNodeInstanceInfo = Some(recommendedNodeInstance)
            recommendedClusterInfo = Some(RecommendedClusterInfo(
              vendor = vendor,
              coresPerExecutor = clusterConfig.coresPerExec,
              numWorkerNodes = numWorkerNodes,
              numGpusPerNode = recommendedNodeInstance.numGpus,
              numExecutors = clusterConfig.numExecutors,
              gpuDevice = getGpuOrDefault.toString,
              dynamicAllocationEnabled = dynamicAllocSettings.enabled,
              dynamicAllocationMaxExecutors = dynamicAllocSettings.max,
              dynamicAllocationMinExecutors = dynamicAllocSettings.min,
              dynamicAllocationInitialExecutors = dynamicAllocSettings.initial,
              workerNodeType = Some(recommendedNodeInstance.name)
            ))

          case None =>
            logWarning("Failed to generate a cluster recommendation. " +
              "Could not determine number of executors. " +
              "Check the Spark properties used for this application or " +
              "cluster properties (if provided).")
        }

      case None =>
        logWarning("Failed to generate a cluster recommendation. " +
          "Could not determine number of executors. " +
          "Cluster properties are missing and event log does not contain cluster information.")
    }
  }
}

abstract class DatabricksPlatform(gpuDevice: Option[GpuDevice],
    clusterProperties: Option[ClusterProperties]) extends Platform(gpuDevice, clusterProperties) {
  override val defaultGpuDevice: GpuDevice = T4Gpu
  override val sparkVersionLabel: String = "Databricks runtime"
  override def isPlatformCSP: Boolean = true

  override val supportedRuntimes: Set[SparkRuntime.SparkRuntime] = Set(
    SparkRuntime.SPARK, SparkRuntime.SPARK_RAPIDS, SparkRuntime.PHOTON
  )

  // note that Databricks generally sets the spark.executor.memory for the user.  Our
  // auto tuner heuristics generally sets it lower then Databricks so go ahead and
  // allow our auto tuner to take affect for this in anticipation that we will use more
  // off heap memory.
  override val recommendationsToExclude: Set[String] = Set(
    "spark.executor.cores",
    "spark.executor.instances",
    "spark.executor.memoryOverhead"
  )

  // Supported Databricks version to RapidsShuffleManager version mapping.
  override val supportedShuffleManagerVersionMap: Array[(String, String)] = Array(
    "11.3" -> "330db",
    "12.2" -> "332db",
    "13.3" -> "341db"
  )

  override def createClusterInfo(coresPerExecutor: Int,
      numExecsPerNode: Int,
      numExecs: Int,
      numWorkerNodes: Int,
      sparkProperties: Map[String, String],
      systemProperties: Map[String, String]): ExistingClusterInfo = {
    val workerNodeType = sparkProperties.get(DatabricksParseHelper.PROP_WORKER_TYPE_ID_KEY)
    val driverNodeType = sparkProperties.get(DatabricksParseHelper.PROP_DRIVER_TYPE_ID_KEY)
    val clusterId = sparkProperties.get(DatabricksParseHelper.PROP_TAG_CLUSTER_ID_KEY)
    val driverHost = sparkProperties.get("spark.driver.host")
    val clusterName = sparkProperties.get(DatabricksParseHelper.PROP_TAG_CLUSTER_NAME_KEY)
    val executorHeapMem = getExecutorHeapMemoryMB(sparkProperties)
    val dynamicAllocSettings = Platform.getDynamicAllocationSettings(sparkProperties)
    ExistingClusterInfo(platformName, coresPerExecutor, numExecsPerNode, numExecs, numWorkerNodes,
      executorHeapMem, dynamicAllocSettings.enabled, dynamicAllocSettings.max,
      dynamicAllocSettings.min, dynamicAllocSettings.initial, driverNodeType,
      workerNodeType, driverHost, clusterId, clusterName)
  }
}

class DatabricksAwsPlatform(gpuDevice: Option[GpuDevice],
    clusterProperties: Option[ClusterProperties])
  extends DatabricksPlatform(gpuDevice, clusterProperties)
  with Logging {
  override val platformName: String = PlatformNames.DATABRICKS_AWS
  override val defaultGpuDevice: GpuDevice = A10GGpu

  override def getInstanceByResourcesMap: Map[(Int, Int), InstanceInfo] = {
    PlatformInstanceTypes.DATABRICKS_AWS_BY_GPUS_CORES
  }
}

class DatabricksAzurePlatform(gpuDevice: Option[GpuDevice],
    clusterProperties: Option[ClusterProperties])
  extends DatabricksPlatform(gpuDevice, clusterProperties) {
  override val platformName: String = PlatformNames.DATABRICKS_AZURE

  override def maxGpusSupported: Int = 4

  override def getInstanceByResourcesMap: Map[(Int, Int), InstanceInfo] = {
    PlatformInstanceTypes.AZURE_NCAS_T4_V3_BY_GPUS_CORES
  }
}

class DataprocPlatform(gpuDevice: Option[GpuDevice],
    clusterProperties: Option[ClusterProperties]) extends Platform(gpuDevice, clusterProperties) {
  override val platformName: String = PlatformNames.DATAPROC
  override val defaultGpuDevice: GpuDevice = T4Gpu
  override val recommendationsToInclude: Seq[(String, String)] = Seq(
    // Keep disabled. This property does not work well with GPU clusters.
    "spark.dataproc.enhanced.optimizer.enabled" -> "false",
    // Keep disabled. This property does not work well with GPU clusters.
    "spark.dataproc.enhanced.execution.enabled" -> "false"
  )

  override def isPlatformCSP: Boolean = true
  override def maxGpusSupported: Int = 4

  override def getInstanceByResourcesMap: Map[(Int, Int), InstanceInfo] = {
    PlatformInstanceTypes.DATAPROC_BY_GPUS_CORES
  }
}

class DataprocServerlessPlatform(gpuDevice: Option[GpuDevice],
    clusterProperties: Option[ClusterProperties])
  extends DataprocPlatform(gpuDevice, clusterProperties) {
  override val platformName: String = PlatformNames.DATAPROC_SL
  override val defaultGpuDevice: GpuDevice = L4Gpu
  override def isPlatformCSP: Boolean = true
}

class DataprocGkePlatform(gpuDevice: Option[GpuDevice],
    clusterProperties: Option[ClusterProperties])
  extends DataprocPlatform(gpuDevice, clusterProperties) {
  override val platformName: String = PlatformNames.DATAPROC_GKE
  override def isPlatformCSP: Boolean = true
}

class EmrPlatform(gpuDevice: Option[GpuDevice],
    clusterProperties: Option[ClusterProperties]) extends Platform(gpuDevice, clusterProperties) {
  override val platformName: String = PlatformNames.EMR
  override val defaultGpuDevice: GpuDevice = A10GGpu

  override def isPlatformCSP: Boolean = true
  override def requirePathRecommendations: Boolean = false

  override def getRetainedSystemProps: Set[String] = Set("EMR_CLUSTER_ID")

  override def createClusterInfo(coresPerExecutor: Int,
      numExecsPerNode: Int,
      numExecs: Int,
      numWorkerNodes: Int,
      sparkProperties: Map[String, String],
      systemProperties: Map[String, String]): ExistingClusterInfo = {
    val clusterId = systemProperties.get("EMR_CLUSTER_ID")
    val driverHost = sparkProperties.get("spark.driver.host")
    val executorHeapMem = getExecutorHeapMemoryMB(sparkProperties)
    val dynamicAllocSettings = Platform.getDynamicAllocationSettings(sparkProperties)
    ExistingClusterInfo(platformName, coresPerExecutor, numExecsPerNode, numExecs,
      numWorkerNodes, executorHeapMem, dynamicAllocSettings.enabled, dynamicAllocSettings.max,
      dynamicAllocSettings.min, dynamicAllocSettings.initial, clusterId = clusterId,
      driverHost = driverHost)
  }

  override def getInstanceByResourcesMap: Map[(Int, Int), InstanceInfo] = {
    PlatformInstanceTypes.EMR_BY_GPUS_CORES
  }
}

class OnPremPlatform(gpuDevice: Option[GpuDevice],
    clusterProperties: Option[ClusterProperties]) extends Platform(gpuDevice, clusterProperties) {
  override val platformName: String = PlatformNames.ONPREM
  // Note we don't have an speedup factor file for onprem l4's but we want auto tuner
  // to use L4.
  override val defaultGpuDevice: GpuDevice = L4Gpu
  override val defaultGpuForSpeedupFactor: GpuDevice = A100Gpu
  // on prem is hard since we don't know what node configurations they have
  // assume 1 for now. We should have them pass this information in the future.
  override def maxGpusSupported: Int = 1
}

object Platform {
  def isDynamicAllocationEnabled(sparkProperties: Map[String, String]): Boolean = {
    sparkProperties.getOrElse("spark.dynamicAllocation.enabled", "false").toBoolean
  }

  def getDynamicAllocationSettings(sparkProperties: Map[String, String]): DynamicAllocationInfo = {
    val dynamicAllocationEnabled = isDynamicAllocationEnabled(sparkProperties)
    if (dynamicAllocationEnabled) {
      val dynamicAllocationMax = sparkProperties.
        getOrElse("spark.dynamicAllocation.maxExecutors", Int.MaxValue.toString)
      val dynamicAllocationMin = sparkProperties.
        getOrElse("spark.dynamicAllocation.minExecutors", "0")
      val dynamicAllocationInit = sparkProperties.
        getOrElse("spark.dynamicAllocation.initialExecutors", sparkProperties.
          getOrElse("spark.executor.instances", dynamicAllocationMin))
      DynamicAllocationInfo(dynamicAllocationEnabled, dynamicAllocationMax,
        dynamicAllocationMin, dynamicAllocationInit)
    } else {
      DynamicAllocationInfo(dynamicAllocationEnabled, "N/A", "N/A", "N/A")
    }
  }
}

/**
 * Factory for creating instances of different platforms.
 * This factory supports various platforms and provides methods for creating
 * corresponding platform instances.
 */
object PlatformFactory extends Logging {
  /**
   * Extracts platform and GPU names from the provided platform key.
   * Assumption: If the last part contains a number, we assume it is GPU name
   *
   * E.g.,
   * - 'emr-t4': Platform: emr, GPU: t4
   * - 'dataproc-gke-l4': Platform dataproc-gke, GPU: l4
   * - 'databricks-aws': Platform databricks-aws, GPU: None
   */
  private def extractPlatformGpuName(platformKey: String): (String, Option[String]) = {
    val parts = platformKey.split('-')
    val numberPattern = ".*\\d.*".r
    // If the last part contains a number, we assume it is GPU name
    if (numberPattern.findFirstIn(parts.last).isDefined) {
      (parts.init.toList.mkString("-"), Some(parts.last))
    } else {
      // If no GPU information is present, return the entire platform key as the
      // platform name and None for GPU
      (parts.toList.mkString("-"), None)
    }
  }

  @throws[IllegalArgumentException]
  @tailrec
  private def createPlatformInstance(platformName: String,
      gpuDevice: Option[GpuDevice],
      clusterProperties: Option[ClusterProperties]): Platform = platformName match {
    case PlatformNames.DATABRICKS_AWS => new DatabricksAwsPlatform(gpuDevice, clusterProperties)
    case PlatformNames.DATABRICKS_AZURE => new DatabricksAzurePlatform(gpuDevice, clusterProperties)
    case PlatformNames.DATAPROC => new DataprocPlatform(gpuDevice, clusterProperties)
    case PlatformNames.DATAPROC_GKE => new DataprocGkePlatform(gpuDevice, clusterProperties)
    case PlatformNames.DATAPROC_SL => new DataprocServerlessPlatform(gpuDevice, clusterProperties)
    case PlatformNames.EMR => new EmrPlatform(gpuDevice, clusterProperties)
    case PlatformNames.ONPREM => new OnPremPlatform(gpuDevice, clusterProperties)
    case p if p.isEmpty =>
      logInfo(s"Platform is not specified. Using ${PlatformNames.DEFAULT} as default.")
      createPlatformInstance(PlatformNames.DEFAULT, gpuDevice, clusterProperties)
    case _ =>
      throw new IllegalArgumentException(s"Unsupported platform: $platformName. " +
        s"Options include ${PlatformNames.getAllNames.mkString(", ")}.")
  }

  /**
   * Creates an instance of `Platform` based on the specified platform key.
   *
   * @param platformKey The key identifying the platform. Defaults to `PlatformNames.DEFAULT`.
   * @param clusterProperties Optional cluster properties if the user specified them.
   */
  def createInstance(platformKey: String = PlatformNames.DEFAULT,
      clusterProperties: Option[ClusterProperties] = None): Platform = {
    val (platformName, gpuName) = extractPlatformGpuName(platformKey)
    val gpuDevice = gpuName.flatMap(GpuDevice.createInstance)
    // case when gpu name is detected but not in device map
    if (gpuName.isDefined && gpuDevice.isEmpty) {
      throw new IllegalArgumentException(s"Unsupported GPU device: ${gpuName.get}. " +
          s"Supported GPU devices are: ${GpuDevice.deviceMap.keys.mkString(", ")}.")
    }
    val platform = createPlatformInstance(platformName, gpuDevice, clusterProperties)
    logInfo(s"Using platform: $platform")
    platform
  }
}
