/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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
import com.nvidia.spark.rapids.tool.profiling.ClusterProperties

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.{ExistingClusterInfo, RecommendedClusterInfo}
import org.apache.spark.sql.rapids.tool.util.StringUtils

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
case class InstanceInfo(cores: Int, memoryMB: Long, name: String, numGpus: Int)

// This is meant to be temporary mapping to figure out instance type based
// on the number of GPUs and cores.  Eventually this should be read from files
// generated based on CSP instance information.
// format (numGpus, numCores) -> InstanceInfo about that CSP node instance type
object PlatformInstanceTypes {

  val AWS_BY_GPUS_CORES = Map((1, 4) -> InstanceInfo(4, 16 * 1024, "g5.xlarge", 1),
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

  // It's not deal to use vars here but to minimize changes and
  // keep backwards compatibility we put them here for now and hopefully
  // in future we can refactor.
  var clusterInfoFromEventLog: Option[ExistingClusterInfo] = None
  // instance information for the gpu node type we will use to run with
  var recommendedNodeInstanceInfo: Option[InstanceInfo] = None
  // overall final recommended cluster configuration
  var recommendedClusterInfo: Option[RecommendedClusterInfo] = None
  // the number of GPUs to use, this might be updated as we handle different cases
  var numGpus: Int = 1

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

  final def setNumGpus(numGpus: Int): Unit = {
    if (numGpus > 1) {
      this.numGpus = numGpus
    }
  }

  /**
   * Important system properties that should be retained based on platform.
   */
  def getRetainedSystemProps: Set[String] = Set.empty


  def getExecutorHeapMemoryMB(sparkProperties: Map[String, String]): Long = {
    // Potentially enhance this to handle if no config then check the executor
    // added or resource profile added events for the heap size
    val executorMemoryFromConf = sparkProperties.get("spark.executor.memory")
    if (executorMemoryFromConf.isDefined) {
      StringUtils.convertToMB(executorMemoryFromConf.getOrElse("0"))
    } else {
      val sparkMasterConf = sparkProperties.get("spark.master")
      sparkMasterConf match {
        case None => 0L
        case Some(sparkMaster) =>
          if (sparkMaster.contains("yarn")) {
            StringUtils.convertToMB("1g")
          } else if (sparkMaster.contains("k8s")) {
            StringUtils.convertToMB("1g")
          } else if (sparkMaster.startsWith("spark:")) {
            // would be the entire node memory by default
            0L
          } else {
            // local mode covered here - do we want to handle specifically?
            0L
          }
      }
    }
  }

  def getExecutorOverheadMemoryMB(sparkProperties: Map[String, String]): Long = {
    val executorMemoryOverheadFromConf = sparkProperties.get("spark.executor.memoryOverhead")
    val execMemOverheadFactorFromConf = sparkProperties.get("spark.executor.memoryOverheadFactor")
    val execHeapMemoryMB = getExecutorHeapMemoryMB(sparkProperties)
    if (executorMemoryOverheadFromConf.isDefined) {
      StringUtils.convertToMB(executorMemoryOverheadFromConf.get)
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

  def getNumGPUsPerNode(): Int = {
    val gpus = if (clusterProperties.isDefined) {
      clusterProperties.get.gpu.getCount
    } else {
      // assume using 1 GPU per node unless specified
      recommendedNodeInstanceInfo.map(_.numGpus).getOrElse(1)
    }
    math.max(1, gpus)
  }

  // Get the number of nodes that were used in the source cluster.
  def getSourceNumNodes(): Int = {
    if (clusterProperties.isDefined) {
      Math.max(1, clusterProperties.get.system.numWorkers)
    } else if (clusterInfoFromEventLog.isDefined) {
      clusterInfoFromEventLog.get.numWorkerNodes
    } else {
      1
    }
  }

  // we want to keep the number of executors used between runs the same
  def getNumExecutorInstances(sparkProperties: Map[String, String]): Int = {
    val dynamicAllocationEnabled = Platform.isDynamicAllocationEnabled(sparkProperties)
    val execInstFromProps = sparkProperties.get("spark.executor.instances")
    // If the cluster properties were specified make sure to use those and not
    // the eventlog inference. This is broken in my mind but is backwards compatible,
    // or maybe use number gpus per node as an improvement.
    if (clusterProperties.isDefined) {
      val numWorkers = Math.max(1, clusterProperties.get.system.numWorkers)
      this.numGpus * numWorkers
    } else if (execInstFromProps.isDefined && !dynamicAllocationEnabled) {
      execInstFromProps.get.toInt
    } else if (clusterInfoFromEventLog.isDefined) {
      clusterInfoFromEventLog.get.numExecutors
    } else {
      // not sure so don't set it
      0
    }
  }

  // figure out memory MB per node when we don't have the specific instance information
  def getMemoryMBPerNode(sparkProperties: Map[String, String]): Long = {
    // To keep backwards compatibility, we first check if cluster properties are defined and
    // use those as the source cluster. This is going to be wrong in many
    // cases if the eventlogs passed in are not all actually run on the same cluster
    // shape. Ideally we change this in the future.
    if (clusterProperties.isDefined) {
      StringUtils.convertToMB(clusterProperties.get.system.getMemory)
    } else if (clusterInfoFromEventLog.isDefined) {
      val numExecutorsPerNode = clusterInfoFromEventLog.map(_.numExecsPerNode)
        .getOrElse(1).toLong
      val heapMemMB = clusterInfoFromEventLog.get.executorHeapMemory
      val overheadMemMB = getExecutorOverheadMemoryMB(sparkProperties)
      (heapMemMB + overheadMemMB) * numExecutorsPerNode
    } else {
      // we don't know
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
   * The maximum number of Gpus any instance in this platform supports.
   */
  def maxGpusSupported: Int = 1

  /**
   * Attempts to get the instance type based on the core and gpu requirements.
   */
  def getInstanceByResources(cores:Int, numGpus: Int): Option[InstanceInfo] = None

  /**
   * Recommend a GPU Instance type to use for this application.
   */
  def getGPUInstanceTypeRecommendation(
      sparkProperties: Map[String, String]): Option[RecommendedClusterInfo] = {
    val vendor = clusterInfoFromEventLog.map(_.vendor).getOrElse("")
    val initialNumExecInstances = getNumExecutorInstances(sparkProperties)
    // If the cluster properties were specified make sure to use those and not
    // the eventlog inference. This is broken in my mind but is backwards compatible,
    // or maybe use number gpus per node as an improvement.
    val numExecsPerNode = if (clusterProperties.isEmpty) {
      // numExecsPerNode can be -1 if dynamic allocation so just make it 1 for
      // this set of calculations
      Math.max(clusterInfoFromEventLog.map(_.numExecsPerNode).getOrElse(1), 1)
    } else {
      1
    }
    // onprem yarn multi-tenant vs yarn static cluster (dataproc) for just that application
    // should be handled automatically unless heterogeneous nodes
    val gpusToUse =
      Math.max(this.numGpus, Math.min(numExecsPerNode, maxGpusSupported))
    // update the global numGpus based on the instance type we are using
    this.numGpus = gpusToUse
    val nodeCores = if (clusterProperties.isDefined) {
      logDebug("Using the cluster properties passed in.")
      // I guess the assumption here is 1 executor per node - or we need to look this up
      // since not in the cluster definition, either way this is number cores per node
      clusterProperties.get.system.getNumCores
    } else if (clusterInfoFromEventLog.isDefined) {
      logDebug("Using the cluster information from the event log.")
      val clusterInfo = clusterInfoFromEventLog.get
      // this assumes this job filled an entire node, which may not be true on
      // a multiple tenant cluster. If the number of executors ran per node would
      // require multiple GPUs per node check to see if this platform supports it.
      // If it doesn't we need to increase the number of nodes recommended.
      clusterInfo.coresPerExecutor * gpusToUse
    } else {
      // shouldn't ever happen
      logError("Cluster properties wasn't specified and cluster information couldn't be " +
        "inferred from the event log!")
      0
    }
    val instanceInfoOpt = getInstanceByResources(nodeCores, gpusToUse)
    val finalInstanceInfo = if (instanceInfoOpt.isEmpty) {
      // if the instance info isn't found, like onprem or some platform we don't know about
      val execCores = if (clusterInfoFromEventLog.isDefined) {
        clusterInfoFromEventLog.get.coresPerExecutor
      } else {
        logWarning("cluster information from event log is missing, executor cores set to 0!")
        0
      }
      val nodeCoresToUse = execCores * gpusToUse
      val nodeMemMB = getMemoryMBPerNode(sparkProperties)
      // It's possible if a cpu run was used, it could run with multiple executors, but
      // if the platform doesn't support multiple GPUs per node then we could recommend
      // different number of nodes. We have to take this into account for cores and memory
      // calculations.
      val ratioExecs = Math.max(1, numExecsPerNode / gpusToUse)
      val execMem = nodeMemMB / ratioExecs
      logDebug(s"Creating instance info execCores $execCores execMem $execMem ratio " +
        s"$ratioExecs numExecsPerNode $numExecsPerNode gpusToUse $gpusToUse")
      // here we change instanceInfo to be executor because assumption is it's on prem and we
      // don't know how to recommend node type
      Some(InstanceInfo(nodeCoresToUse, execMem, "onprem", 1))
    } else if (clusterProperties.isDefined) {
      val info = instanceInfoOpt.get
      // make sure that instanceInfo matches the cluster properties else change
      val clusterPropMemMB = StringUtils.convertToMB(clusterProperties.get.system.getMemory)
      if (info.cores == nodeCores && info.memoryMB == clusterPropMemMB) {
        instanceInfoOpt
      } else {
        Some(InstanceInfo(nodeCores, clusterPropMemMB, info.name, info.numGpus))
      }
    } else {
      instanceInfoOpt
    }
    val numExistingNodes = getSourceNumNodes
    // check if instance type supports that number of gpus, if not we add extra executors
    val (numExecs, numNodes) = if (finalInstanceInfo.get.numGpus >= numExecsPerNode) {
      // TODO - really if instance has more GPUs we should calculate the other way to
      // recommend less nodes but leave that open for now
      (initialNumExecInstances, numExistingNodes)
    } else {
      // just flatten to use 1 but we should really see if multiples
      val numGpusLeft = numExecsPerNode / finalInstanceInfo.get.numGpus
      (initialNumExecInstances, numExistingNodes * numGpusLeft)
    }
    val coresPerExec = if (finalInstanceInfo.isDefined) {
      finalInstanceInfo.get.cores / finalInstanceInfo.get.numGpus
    } else {
      1
    }
    val finalNumNodes = if (vendor == PlatformNames.ONPREM) {
      // if its onprem we really have no idea of the size of the cluster
      -1
    } else {
      numNodes
    }
    if (numExecs > 0) {
      val instanceName = finalInstanceInfo.map(_.name).getOrElse("")
      val numGpus = finalInstanceInfo.map(_.numGpus).getOrElse(1)
      val dynamicAllocSettings = Platform.getDynamicAllocationSettings(sparkProperties)

      // Num of executors per node is the number of GPUs
      recommendedClusterInfo = Some(RecommendedClusterInfo(vendor, coresPerExec,
        finalNumNodes, numGpus, numExecs, gpuDevice = getGpuOrDefault.toString,
        dynamicAllocSettings.enabled, dynamicAllocSettings.max,
        dynamicAllocSettings.min, dynamicAllocSettings.initial,
        workerNodeType = Some(instanceName)))
      recommendedNodeInstanceInfo = finalInstanceInfo
      recommendedClusterInfo
    } else {
      logWarning("No executors so the recommended cluster and node instance information" +
        " is not set!")
      None
    }
  }
}

abstract class DatabricksPlatform(gpuDevice: Option[GpuDevice],
    clusterProperties: Option[ClusterProperties]) extends Platform(gpuDevice, clusterProperties) {
  override val defaultGpuDevice: GpuDevice = T4Gpu
  override def isPlatformCSP: Boolean = true

  // note that Databricks generally sets the spark.executor.memory for the user.  Our
  // auto tuner heuristics generally sets it lower then Databricks so go ahead and
  // allow our auto tuner to take affect for this in anticipation that we will use more
  // off heap memory.
  override val recommendationsToExclude: Set[String] = Set(
    "spark.executor.cores",
    "spark.executor.instances",
    "spark.executor.memoryOverhead"
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
  override val platformName: String =  PlatformNames.DATABRICKS_AWS
  override val defaultGpuDevice: GpuDevice = A10GGpu

  override def getInstanceByResources(
      cores: Int, numGpus: Int): Option[InstanceInfo] = {
    val exactInstance = PlatformInstanceTypes.AWS_BY_GPUS_CORES.get((numGpus, cores))
    if (exactInstance.isEmpty) {
      // try to find the closest
      val enoughGpus = PlatformInstanceTypes.AWS_BY_GPUS_CORES.filterKeys { gpuCores =>
        gpuCores._1 >= numGpus && gpuCores._2 >= cores
      }
      if (enoughGpus.isEmpty) {
        None
      } else {
        // add the gpus and cores to get a minimum value that matched.
        val res = enoughGpus.keys.minBy(x => x._1 + x._2)
        enoughGpus.get(res)
      }
    } else {
      exactInstance
    }
  }
}

class DatabricksAzurePlatform(gpuDevice: Option[GpuDevice],
    clusterProperties: Option[ClusterProperties])
  extends DatabricksPlatform(gpuDevice, clusterProperties) {
  override val platformName: String = PlatformNames.DATABRICKS_AZURE

  override def maxGpusSupported: Int = 4

  override def getInstanceByResources(
      cores: Int, numGpus: Int): Option[InstanceInfo] = {
    val exactInstance = PlatformInstanceTypes.AZURE_NCAS_T4_V3_BY_GPUS_CORES.get((numGpus, cores))
    if (exactInstance.isEmpty) {
      // try to find the closest
      val enoughGpus = PlatformInstanceTypes.AZURE_NCAS_T4_V3_BY_GPUS_CORES.filterKeys { gpuCores =>
        gpuCores._1 >= numGpus && gpuCores._2 >= cores
      }
      if (enoughGpus.isEmpty) {
        None
      } else {
        // add the gpus and cores to get a minimum value that matched.
        val res = enoughGpus.keys.minBy(x => x._1 + x._2)
        enoughGpus.get(res)
      }
    } else {
      exactInstance
    }
  }
}

class DataprocPlatform(gpuDevice: Option[GpuDevice],
    clusterProperties: Option[ClusterProperties]) extends Platform(gpuDevice, clusterProperties) {
  override val platformName: String =  PlatformNames.DATAPROC
  override val defaultGpuDevice: GpuDevice = T4Gpu
  override def isPlatformCSP: Boolean = true
  override def maxGpusSupported: Int = 4

  override def getInstanceByResources(
      cores: Int, numGpus: Int): Option[InstanceInfo] = {
    val exactInstance = PlatformInstanceTypes.DATAPROC_BY_GPUS_CORES.get((numGpus, cores))
    if (exactInstance.isEmpty) {
      // try to find the closest
      val enoughGpus = PlatformInstanceTypes.DATAPROC_BY_GPUS_CORES.filterKeys { gpuCores =>
        gpuCores._1 >= numGpus && gpuCores._2 >= cores
      }
      if (enoughGpus.isEmpty) {
        None
      } else {
        // add the gpus and cores to get a minimum value that matched.
        val res = enoughGpus.keys.minBy(x => x._1 + x._2)
        enoughGpus.get(res)
      }
    } else {
      exactInstance
    }
  }
}

class DataprocServerlessPlatform(gpuDevice: Option[GpuDevice],
    clusterProperties: Option[ClusterProperties])
  extends DataprocPlatform(gpuDevice, clusterProperties) {
  override val platformName: String =  PlatformNames.DATAPROC_SL
  override val defaultGpuDevice: GpuDevice = L4Gpu
  override def isPlatformCSP: Boolean = true
}

class DataprocGkePlatform(gpuDevice: Option[GpuDevice],
    clusterProperties: Option[ClusterProperties])
  extends DataprocPlatform(gpuDevice, clusterProperties) {
  override val platformName: String =  PlatformNames.DATAPROC_GKE
  override def isPlatformCSP: Boolean = true
}

class EmrPlatform(gpuDevice: Option[GpuDevice],
    clusterProperties: Option[ClusterProperties]) extends Platform(gpuDevice, clusterProperties) {
  override val platformName: String =  PlatformNames.EMR
  override val defaultGpuDevice: GpuDevice = A10GGpu

  override def isPlatformCSP: Boolean = true

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

  override def getInstanceByResources(
      cores: Int, numGpus: Int): Option[InstanceInfo] = {
    val exactInstance = PlatformInstanceTypes.AWS_BY_GPUS_CORES.get((numGpus, cores))
    if (exactInstance.isEmpty) {
      // try to find the closest
      val enoughGpus = PlatformInstanceTypes.AWS_BY_GPUS_CORES.filterKeys { gpuCores =>
        gpuCores._1 >= numGpus && gpuCores._2 >= cores
      }
      if (enoughGpus.isEmpty) {
        None
      } else {
        // add the gpus and cores to get a minimum value that matched.
        val res = enoughGpus.keys.minBy(x => x._1 + x._2)
        enoughGpus.get(res)
      }
    } else {
      exactInstance
    }
  }
}

class OnPremPlatform(gpuDevice: Option[GpuDevice],
    clusterProperties: Option[ClusterProperties]) extends Platform(gpuDevice, clusterProperties) {
  override val platformName: String =  PlatformNames.ONPREM
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
