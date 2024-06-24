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
import org.apache.spark.sql.rapids.tool.ClusterInfo
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

case class InstanceCoresMemory(cores: Int, memoryMB: Long)

object PlatformInstanceTypes {
  val AWS = Map("large" -> InstanceCoresMemory(2, 8 * 1024),
    "xlarge" -> InstanceCoresMemory(4, 16 * 1024),
    "2xlarge" -> InstanceCoresMemory(8, 32 * 1024),
    "4xlarge" -> InstanceCoresMemory(16, 64 * 1024),
    "8xlarge" -> InstanceCoresMemory(32, 128 * 1024),
    "12xlarge" -> InstanceCoresMemory(48, 192 * 1024), // multiple gpu
    "16xlarge" -> InstanceCoresMemory(64, 256 * 1024)
  )

  // Standard_NC
  val AZURE_NC = Map(
    "12" -> InstanceCoresMemory(12, 112 * 1024),
    "24" -> InstanceCoresMemory(24, 224 * 1024)
  )
  // Standard_NC6s_v3
  val AZURE_NC_V3 = Map(
    "6" -> InstanceCoresMemory(6, 112 * 1024),
    "12" -> InstanceCoresMemory(12, 224 * 1024),
    "24" -> InstanceCoresMemory(24, 448 * 1024)
  )
  // Standard_NC4as_T4_v3
  val AZURE_NCAS_T4_V3 = Map(
    "4" -> InstanceCoresMemory(4, 28 * 1024),
    "8" -> InstanceCoresMemory(8, 56 * 1024),
    "16" -> InstanceCoresMemory(16, 110 * 1024),
    "64" -> InstanceCoresMemory(64, 440 * 1024)
  )

  // dataproc and dataproc-gke
  val DATAPROC = Map("1" -> InstanceCoresMemory(1, 1 * 3840),
    "2" -> InstanceCoresMemory(2, 2 * 3840),
    "4" -> InstanceCoresMemory(4, 4 * 3840),
    "8" -> InstanceCoresMemory(8, 8 * 3840),
    "16" -> InstanceCoresMemory(16, 16 * 3840),
    "32" -> InstanceCoresMemory(32, 32 * 3840),
    "64" -> InstanceCoresMemory(64, 64 * 3840),
    "96" -> InstanceCoresMemory(96, 96 * 3840)
  )

  // TODO - need serverless emr/dataproc
}

/**
 * Represents a platform and its associated recommendations.
 *
 * @param gpuDevice Gpu Device present in the platform
 */
abstract class Platform(var gpuDevice: Option[GpuDevice],
    val clusterProperties: Option[ClusterProperties]) extends Logging {
  val platformName: String
  val defaultGpuDevice: GpuDevice
  var clusterInfoFromEventLog: Option[ClusterInfo] = None

  // This function allow us to have one gpu type used by the auto
  // tuner recommendations but have a different GPU used for speedup
  // factors since we don't have speedup factors for all combinations of
  // platforms and GPUs. We expect speedup factor usage to be going away
  // so this is less of an issue.
  def defaultGpuForSpeedupFactor: GpuDevice = getGpuOrDefault

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

  // TODO - if no config add in check executor heap sizes
  def getExecutorHeapMemory(sparkProperties: Map[String, String]): Long = {
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
            // TODO - local mode covered here - do we want to handle specifically?
            // TODO - any special ones for specific CSPs?
            0L
          }
      }
    }
  }

  def getExecutorOverheadMemoryMB(sparkProperties: Map[String, String]): Long = {
    val executorMemoryOverheadFromConf = sparkProperties.get("spark.executor.memoryOverhead")
    val execMemOverheadFactorFromConf = sparkProperties.get("spark.executor.memoryOverheadFactor")
    val execHeapMemoryMB = getExecutorHeapMemory(sparkProperties)
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
              // TODO - any special ones for specific CSPs?
              0L
            }
        }
      }
    } else {
      0L
    }
  }

  def getNumGPUsPerExecutor(): Int = {
    if (clusterProperties.isDefined) {
      math.max(1, clusterProperties.get.gpu.getCount)
    } else {
      // assume using 1 GPU per node unless specified
      1
    }
  }

  def getNumExecutorInstances(): Int = {
    if (clusterInfoFromEventLog.isDefined) {
      logWarning("Tom using eventlog")
      // TODO - if gpu eventlog use that number, cpu use 1 or platform
      // TODO - anyway to tell from gpu eventlog?
      logWarning("num instances is 1")
      1
    } else if (clusterProperties.isDefined) {
      // assume 1 GPU per machine unless specified
      // TODO - double check this with python side
      val numGpus = Math.max(1, clusterProperties.get.gpu.getCount)
      val numWorkers = Math.max(1, clusterProperties.get.system.numWorkers)
      logWarning("num instances is " + numGpus + " numworksr: "
        + clusterProperties.get.system.numWorkers)
      numGpus * numWorkers
    } else {
      // not sure so don't set it
      logWarning("num instances is 0")

      0
    }
  }

  def getNumCoresPerNode(): Int = {
    // try to use the ones from event log for now first
    if (clusterInfoFromEventLog.isDefined) {
      logWarning("Tom using eventlog")

      if (clusterInfoFromEventLog.get.instanceInfo.isDefined) {
        clusterInfoFromEventLog.get.instanceInfo.get.cores
      } else {
        // this is assume this job filled an entire node, which may not be true on
        // a multiple tenant cluster
        val nodeCores = clusterInfoFromEventLog.get.coresPerExecutor * clusterInfoFromEventLog.get.numExecsPerNode
        val instanceInfo = getInstanceInfoFromResource(nodeCores, None)
        if (!instanceInfo.isDefined) {
          // we either miscalculated cores or we don't have a node type of the same size
          logWarning(s"Number of cores per node was calculated as: ${nodeCores} but " +
            "we don't have an instance mapping for that!")
          // TODO - find closest instance
          nodeCores
        } else {
          nodeCores
        }
      }
    } else if (clusterProperties.isDefined) {
      // if can't get real event log based info then use what the user passes in
      // TODO - make this configurable to override event log
      logWarning("Tom using the cluster props")
      clusterProperties.get.system.getNumCores
    } else {
      logWarning("Tom using 0")

      // use executor cores - or don't recommend???
      0
    }
  }

  def getMemoryMBPerNode(): Long = {
    // try to use the ones from event log for now first
    if (clusterInfoFromEventLog.isDefined) {
      logWarning("Tom using eventlog mem ")

      if (clusterInfoFromEventLog.get.instanceInfo.isDefined) {
        logWarning("TOM using instance memory: " +
          clusterInfoFromEventLog.get.instanceInfo.get.memoryMB)
        clusterInfoFromEventLog.get.instanceInfo.get.memoryMB
      } else {
        val numExecutorsPerNode = if (clusterInfoFromEventLog.isDefined) {
          clusterInfoFromEventLog.get.numExecsPerNode.toLong
        } else {
          1L
        }
        val heapMemMB = clusterInfoFromEventLog.get.executorHeapMemory
        val overheadMemMB = clusterInfoFromEventLog.get.executorOverheadMemoryMB
        logWarning("TOM using cluster info execsper node: " + numExecutorsPerNode +
          "overhead: " + overheadMemMB + " heap : " + heapMemMB)

        val memPerNodeCalc = (heapMemMB + overheadMemMB) * numExecutorsPerNode
        // now lookup the type if possible
        val cores = getNumCoresPerNode()
        val instanceInfo = getInstanceInfoFromResource(cores, Some(memPerNodeCalc))
        logWarning("instance info for cores: " + cores + " mem: " + memPerNodeCalc +
          " is: " + instanceInfo)
        if (instanceInfo.isDefined) {
          instanceInfo.get.memoryMB
        } else {
          memPerNodeCalc
        }
      }
    } else if (clusterProperties.isDefined) {
      logWarning("Tom using cluster properties mem ")

      // if can't get real event log based info then use what the user passes in
      // TODO - make this configurable to override event log
      logWarning("TOM using cluster properties: " + clusterProperties.get.system.getMemory)
      StringUtils.convertToMB(clusterProperties.get.system.getMemory)
    } else {
      logWarning("TOM using 0")
      // we don't know
      0L
    }
  }

  def createClusterInfo(coresPerExecutor: Int, numExecsPerNode: Int, numExecutorNodes: Int,
      sparkProperties: Map[String, String], systemProperties: Map[String, String]): ClusterInfo = {
    val driverHost = sparkProperties.get("spark.driver.host")
    val executorHeapMem = getExecutorHeapMemory(sparkProperties)
    val executorOverheadMem = getExecutorOverheadMemoryMB(sparkProperties)
    ClusterInfo(platformName, coresPerExecutor, numExecsPerNode, numExecutorNodes,
      executorHeapMem, executorOverheadMem,
      getInstanceResources(sparkProperties), driverHost = driverHost)
  }

  def configureClusterInfoFromEventLog(coresPerExecutor: Int, execsPerNode: Int,
      numExecutorNodes: Int, sparkProperties: Map[String, String],
      systemProperties: Map[String, String]): ClusterInfo = {
    clusterInfoFromEventLog = Some(createClusterInfo(coresPerExecutor, execsPerNode,
      numExecutorNodes, sparkProperties, systemProperties))
    clusterInfoFromEventLog.get
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
   * Attempts to get the cores and memory for the node instance type being used.
   */
  def getInstanceResources(sparkProperties: Map[String, String]): Option[InstanceCoresMemory] = None

  def getInstanceInfoFromResource(cores: Int,
      memoryMB: Option[Long]): Option[InstanceCoresMemory] = None
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

  override def createClusterInfo(coresPerExecutor: Int, numExecsPerNode: Int, numExecutorNodes: Int,
      sparkProperties: Map[String, String], systemProperties: Map[String, String]): ClusterInfo = {
    val executorInstance = sparkProperties.get(DatabricksParseHelper.PROP_WORKER_TYPE_ID_KEY)
    val driverInstance = sparkProperties.get(DatabricksParseHelper.PROP_DRIVER_TYPE_ID_KEY)
    val clusterId = sparkProperties.get(DatabricksParseHelper.PROP_TAG_CLUSTER_ID_KEY)
    val driverHost = sparkProperties.get("spark.driver.host")
    val clusterName = sparkProperties.get(DatabricksParseHelper.PROP_TAG_CLUSTER_NAME_KEY)
    val executorHeapMem = getExecutorHeapMemory(sparkProperties)
    val executorOverheadMem = getExecutorOverheadMemoryMB(sparkProperties)

    // todo check execs per node in case user configured differently
    ClusterInfo(platformName, coresPerExecutor, numExecsPerNode, numExecutorNodes,
      executorHeapMem, executorOverheadMem,
      getInstanceResources(sparkProperties), executorInstance, driverInstance,
      driverHost, clusterId, clusterName)
  }
}

class DatabricksAwsPlatform(gpuDevice: Option[GpuDevice],
    clusterProperties: Option[ClusterProperties])
  extends DatabricksPlatform(gpuDevice, clusterProperties)
  with Logging {
  override val platformName: String =  PlatformNames.DATABRICKS_AWS
  override val defaultGpuDevice: GpuDevice = A10GGpu


  override def getInstanceResources(
      sparkProperties: Map[String, String]): Option[InstanceCoresMemory] = {
    val executorInstance = sparkProperties.get(DatabricksParseHelper.PROP_WORKER_TYPE_ID_KEY)
    logWarning("executor instance is: " + executorInstance)
    if (executorInstance.isDefined) {
      val NODE_REGEX = """[a-z0-9]*\.(\d{1,2}xlarge)""".r
      val nodeSizeMatch = NODE_REGEX.findFirstMatchIn(executorInstance.get).get
      if (nodeSizeMatch.subgroups.size >= 1) {
        val nodeSize = nodeSizeMatch.group(1)
        logWarning("node size is: " + nodeSize)
        PlatformInstanceTypes.AWS.get(nodeSize)
      } else {
        logWarning("couldn't match node size")
        None
      }
    } else {
      None
    }
  }
}

class DatabricksAzurePlatform(gpuDevice: Option[GpuDevice],
    clusterProperties: Option[ClusterProperties])
  extends DatabricksPlatform(gpuDevice, clusterProperties) {
  override val platformName: String =  PlatformNames.DATABRICKS_AZURE

  override def getInstanceResources(
      sparkProperties: Map[String, String]): Option[InstanceCoresMemory] = {
    val executorInstance = sparkProperties.get(DatabricksParseHelper.PROP_WORKER_TYPE_ID_KEY)
    if (executorInstance.isDefined) {
      val STANDARD_NC_NODE_REGEX = """Standard_NC(\d{1,2})r*""".r
      val STANDARD_NC_V3_NODE_REGEX = """Standard_NC(\d{1,2})s_v3""".r
      val STANDARD_NCAS_T4_V3_NODE_REGEX = """Standard_NC(\d{1,2})as_T4_v3""".r
      executorInstance.get match {
        case STANDARD_NC_NODE_REGEX(a) =>
          PlatformInstanceTypes.AZURE_NC.get(a)
        case STANDARD_NC_V3_NODE_REGEX(a) =>
          PlatformInstanceTypes.AZURE_NC_V3.get(a)
        case STANDARD_NCAS_T4_V3_NODE_REGEX(a) =>
          PlatformInstanceTypes.AZURE_NCAS_T4_V3.get(a)
        case _ => None
      }
    } else {
      None
    }
  }

}

class DataprocPlatform(gpuDevice: Option[GpuDevice],
    clusterProperties: Option[ClusterProperties]) extends Platform(gpuDevice, clusterProperties) {
  override val platformName: String =  PlatformNames.DATAPROC
  override val defaultGpuDevice: GpuDevice = T4Gpu
  override def isPlatformCSP: Boolean = true

  override def getInstanceResources(
      sparkProperties: Map[String, String]): Option[InstanceCoresMemory] = {
    // TODO - what is dataproc way to get instance type?
    // not seeing a way to do this might require user input!!!!
    /*
    val executorInstance = sparkProperties.get(DatabricksParseHelper.PROP_WORKER_TYPE_ID_KEY)
    if (executorInstance.isDefined) {
      val NODE_REGEX = """(a-z)*\.(\d){1,2}xlarge""".r
      val nodeSizeMatch = NODE_REGEX.findFirstMatchIn(executorInstance.get).get
      if (nodeSizeMatch.subgroups.size >= 1) {
        val nodeSize = nodeSizeMatch.group(1)
        PlatformInstanceTypes.DATAPROC.get(nodeSize)
      } else {
        None
      }
    } else {
      None
    }

     */
    None
  }

  override def getInstanceInfoFromResource(cores: Int,
      memoryMB: Option[Long]): Option[InstanceCoresMemory] = {
    val instanceType = PlatformInstanceTypes.DATAPROC.get(cores.toString)
    if (instanceType.isDefined) {
      if (memoryMB.isDefined) {
        // make sure memory size within 75%
        if (instanceType.get.memoryMB >= (memoryMB.get * .75)) {
          instanceType
        } else {
          // look for bigger size, we know it doubles in size but probably find
          // better algo
          // skip checking memory after this
          PlatformInstanceTypes.DATAPROC.get(cores.toString * 2)
        }
      } else {
        instanceType
      }
    } else {
      None
    }
  }

}

class DataprocServerlessPlatform(gpuDevice: Option[GpuDevice],
    clusterProperties: Option[ClusterProperties])
  extends DataprocPlatform(gpuDevice, clusterProperties) {
  override val platformName: String =  PlatformNames.DATAPROC_SL
  override val defaultGpuDevice: GpuDevice = L4Gpu
  override def isPlatformCSP: Boolean = true

  override def getInstanceResources(
      sparkProperties: Map[String, String]): Option[InstanceCoresMemory] = {
    None
  }
}

class DataprocGkePlatform(gpuDevice: Option[GpuDevice],
    clusterProperties: Option[ClusterProperties])
  extends DataprocPlatform(gpuDevice, clusterProperties) {
  override val platformName: String =  PlatformNames.DATAPROC_GKE
  override def isPlatformCSP: Boolean = true

  override def getInstanceResources(
      sparkProperties: Map[String, String]): Option[InstanceCoresMemory] = {
    None
  }
}

class EmrPlatform(gpuDevice: Option[GpuDevice],
    clusterProperties: Option[ClusterProperties]) extends Platform(gpuDevice, clusterProperties) {
  override val platformName: String =  PlatformNames.EMR
  override val defaultGpuDevice: GpuDevice = A10GGpu
  override def isPlatformCSP: Boolean = true

  override def getInstanceResources(
      sparkProperties: Map[String, String]): Option[InstanceCoresMemory] = {
    None
  }

  override def getRetainedSystemProps: Set[String] = Set("EMR_CLUSTER_ID")

  override def createClusterInfo(coresPerExecutor: Int, numExecsPerNode: Int, numExecutorNodes: Int,
      sparkProperties: Map[String, String], systemProperties: Map[String, String]): ClusterInfo = {
    val clusterId = systemProperties.get("EMR_CLUSTER_ID")
    val driverHost = sparkProperties.get("spark.driver.host")
    val executorHeapMem = getExecutorHeapMemory(sparkProperties)
    val executorOverheadMem = getExecutorOverheadMemoryMB(sparkProperties)

    ClusterInfo(platformName, coresPerExecutor, numExecsPerNode, numExecutorNodes,
      executorHeapMem, executorOverheadMem,
      instanceInfo = getInstanceResources(sparkProperties),
      clusterId = clusterId, driverHost = driverHost)
  }
}

class OnPremPlatform(gpuDevice: Option[GpuDevice],
    clusterProperties: Option[ClusterProperties]) extends Platform(gpuDevice, clusterProperties) {
  override val platformName: String =  PlatformNames.ONPREM
  // Note we don't have an speedup factor file for onprem l4's but we want auto tuner
  // to use L4.
  override val defaultGpuDevice: GpuDevice = L4Gpu
  override val defaultGpuForSpeedupFactor: GpuDevice = A100Gpu

  override def getInstanceResources(
      sparkProperties: Map[String, String]): Option[InstanceCoresMemory] = {
    None
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
   */
  def createInstance(platformKey: String = PlatformNames.DEFAULT,
      clusterProperties: Option[ClusterProperties] = None): Platform = {
    val (platformName, gpuName) = extractPlatformGpuName(platformKey)
    val gpuDevice = gpuName.flatMap(GpuDevice.createInstance)
    val platform = createPlatformInstance(platformName, gpuDevice, clusterProperties)
    logInfo(s"Using platform: $platform")
    platform
  }
}
