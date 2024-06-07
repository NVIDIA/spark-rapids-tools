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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.ClusterInfo

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
    DATABRICKS_AWS, DATABRICKS_AZURE, DATAPROC, EMR, ONPREM,
    s"$DATAPROC-$L4Gpu", s"$DATAPROC-$T4Gpu",
    s"$DATAPROC_GKE-$L4Gpu", s"$DATAPROC_GKE-$T4Gpu",
    s"$DATAPROC_SL-$L4Gpu", s"$EMR-$A10Gpu", s"$EMR-$T4Gpu"
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
abstract class Platform(var gpuDevice: Option[GpuDevice]) {
  val platformName: String
  val defaultGpuDevice: GpuDevice
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

  def getExecutorHeapMemory(sparkProperties: Map[String, String]): Option[String] = {
    val executorMemoryFromConf = sparkProperties.get("spark.executor.memory")
    if (executorMemoryFromConf.isDefined) {
      executorMemoryFromConf
    } else {
      val sparkMasterConf = sparkProperties.get("spark.master")
      sparkMasterConf match {
        case None => None
        case Some(sparkMaster) =>
          if (sparkMaster.contains("yarn")) {
            Some("1g")
          } else if (sparkMaster.contains("k8s")) {
            Some("1g")
          } else if (sparkMaster.startsWith("spark:")) {
            // would be the entire node memory by default
            None
          } else {
            // TODO - any special ones for specific CSPs?
            None
          }
      }
    }
  }

  /*
  def getExecutorTotalMemory(sparkProperties: Map[String, String]): Option[String] = {
    val heapMemory = getExecutorHeapMemory(sparkProperties)
    val execOverheadMemoryFromConf = sparkProperties.get("spark.executor.memoryOverhead")
    val execOverheadMemoryFactorFromConf =
      sparkProperties.get("spark.executor.memoryOverheadFactor")

    if (execOverheadMemoryFromConf.isDefined) {
      execOverheadMemoryFromConf
    } else {
      val sparkMasterConf = sparkProperties.get("spark.master")
      sparkMasterConf match {
        case None => None
        case Some(sparkMaster) =>
          if (sparkMaster.contains("yarn")) {
            Some("1g")
          } else if (sparkMaster.contains("k8s")) {
            val execOverheadMemoryFactorFromConf =
              sparkProperties.get("spark.kubernetes.memoryOverheadFactor")
            Some("1g")
          } else if (sparkMaster.startsWith("spark:")) {
            // would be the entire node memory by default
            None
          } else {
            // TODO - any special ones for specific CSPs?
            None
          }
      }
    }
  }

   */

  def createClusterInfo(coresPerExecutor: Int, numExecutorNodes: Int,
      sparkProperties: Map[String, String], systemProperties: Map[String, String]): ClusterInfo = {
    val driverHost = sparkProperties.get("spark.driver.host")
    val executorHeapMem = getExecutorHeapMemory(sparkProperties)
    ClusterInfo(platformName, coresPerExecutor, numExecutorNodes, executorHeapMem,
      getInstanceResources(sparkProperties), driverHost = driverHost)
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
}

abstract class DatabricksPlatform(gpuDevice: Option[GpuDevice]) extends Platform(gpuDevice) {
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


  override def createClusterInfo(coresPerExecutor: Int, numExecutorNodes: Int,
      sparkProperties: Map[String, String], systemProperties: Map[String, String]): ClusterInfo = {
    val executorInstance = sparkProperties.get(DatabricksParseHelper.PROP_WORKER_TYPE_ID_KEY)
    val driverInstance = sparkProperties.get(DatabricksParseHelper.PROP_DRIVER_TYPE_ID_KEY)
    val clusterId = sparkProperties.get(DatabricksParseHelper.PROP_TAG_CLUSTER_ID_KEY)
    val driverHost = sparkProperties.get("spark.driver.host")
    val clusterName = sparkProperties.get(DatabricksParseHelper.PROP_TAG_CLUSTER_NAME_KEY)
    val executorHeapMem = getExecutorHeapMemory(sparkProperties)

    ClusterInfo(platformName, coresPerExecutor, numExecutorNodes, executorHeapMem,
      getInstanceResources(sparkProperties), executorInstance, driverInstance,
      driverHost, clusterId, clusterName)
  }
}

class DatabricksAwsPlatform(gpuDevice: Option[GpuDevice]) extends DatabricksPlatform(gpuDevice)
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

class DatabricksAzurePlatform(gpuDevice: Option[GpuDevice]) extends DatabricksPlatform(gpuDevice) {
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

class DataprocPlatform(gpuDevice: Option[GpuDevice]) extends Platform(gpuDevice) {
  override val platformName: String =  PlatformNames.DATAPROC
  override val defaultGpuDevice: GpuDevice = T4Gpu
  override def isPlatformCSP: Boolean = true

  override def getInstanceResources(
      sparkProperties: Map[String, String]): Option[InstanceCoresMemory] = {
    // TODO - what is dataproc way to get insatnce type?
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
}

class DataprocServerlessPlatform(gpuDevice: Option[GpuDevice]) extends DataprocPlatform(gpuDevice) {
  override val platformName: String =  PlatformNames.DATAPROC_SL
  override val defaultGpuDevice: GpuDevice = L4Gpu
  override def isPlatformCSP: Boolean = true

  override def getInstanceResources(
      sparkProperties: Map[String, String]): Option[InstanceCoresMemory] = {
    None
  }
}

class DataprocGkePlatform(gpuDevice: Option[GpuDevice]) extends DataprocPlatform(gpuDevice) {
  override val platformName: String =  PlatformNames.DATAPROC_GKE
  override def isPlatformCSP: Boolean = true

  override def getInstanceResources(
      sparkProperties: Map[String, String]): Option[InstanceCoresMemory] = {
    None
  }
}

class EmrPlatform(gpuDevice: Option[GpuDevice]) extends Platform(gpuDevice) {
  override val platformName: String =  PlatformNames.EMR
  override val defaultGpuDevice: GpuDevice = A10GGpu
  override def isPlatformCSP: Boolean = true

  override def getInstanceResources(
      sparkProperties: Map[String, String]): Option[InstanceCoresMemory] = {
    None
  }

  override def getRetainedSystemProps: Set[String] = Set("EMR_CLUSTER_ID")

  override def createClusterInfo(coresPerExecutor: Int, numExecutorNodes: Int,
      sparkProperties: Map[String, String], systemProperties: Map[String, String]): ClusterInfo = {
    val clusterId = systemProperties.get("EMR_CLUSTER_ID")
    val driverHost = sparkProperties.get("spark.driver.host")
    val executorHeapMem = getExecutorHeapMemory(sparkProperties)
    ClusterInfo(platformName, coresPerExecutor, numExecutorNodes, executorHeapMem,
      instanceInfo = getInstanceResources(sparkProperties),
      clusterId = clusterId, driverHost = driverHost)
  }
}

class OnPremPlatform(gpuDevice: Option[GpuDevice]) extends Platform(gpuDevice) {
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
      gpuDevice: Option[GpuDevice]): Platform = platformName match {
    case PlatformNames.DATABRICKS_AWS => new DatabricksAwsPlatform(gpuDevice)
    case PlatformNames.DATABRICKS_AZURE => new DatabricksAzurePlatform(gpuDevice)
    case PlatformNames.DATAPROC => new DataprocPlatform(gpuDevice)
    case PlatformNames.DATAPROC_GKE => new DataprocGkePlatform(gpuDevice)
    case PlatformNames.DATAPROC_SL => new DataprocServerlessPlatform(gpuDevice)
    case PlatformNames.EMR => new EmrPlatform(gpuDevice)
    case PlatformNames.ONPREM => new OnPremPlatform(gpuDevice)
    case p if p.isEmpty =>
      logInfo(s"Platform is not specified. Using ${PlatformNames.DEFAULT} as default.")
      createPlatformInstance(PlatformNames.DEFAULT, gpuDevice)
    case _ =>
      throw new IllegalArgumentException(s"Unsupported platform: $platformName. " +
        s"Options include ${PlatformNames.getAllNames.mkString(", ")}.")
  }

  /**
   * Creates an instance of `Platform` based on the specified platform key.
   *
   * @param platformKey The key identifying the platform. Defaults to `PlatformNames.DEFAULT`.
   */
  def createInstance(platformKey: String = PlatformNames.DEFAULT): Platform = {
    val (platformName, gpuName) = extractPlatformGpuName(platformKey)
    val gpuDevice = gpuName.flatMap(GpuDevice.createInstance)
    val platform = createPlatformInstance(platformName, gpuDevice)
    logInfo(s"Using platform: $platform")
    platform
  }
}
