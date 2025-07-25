/*
 * Copyright (c) 2022-2025, NVIDIA CORPORATION.
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

import scala.beans.BeanProperty
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable
import scala.util.control.NonFatal
import scala.util.matching.Regex

import com.nvidia.spark.rapids.tool.{AppSummaryInfoBaseProvider, ClusterSizingStrategy, ConstantGpuCountStrategy, GpuDevice, Platform, PlatformFactory}
import com.nvidia.spark.rapids.tool.profiling._
import org.yaml.snakeyaml.constructor.ConstructorException

import org.apache.spark.internal.Logging
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.sql.rapids.tool.ToolUtils
import org.apache.spark.sql.rapids.tool.util.{PropertiesLoader, StringUtils, UTF8Source, ValidatableProperties, WebCrawlerUtil}

/**
 * A wrapper class that stores all the GPU properties.
 * The BeanProperty enables loading and parsing the YAML formatted content using the
 * Constructor SnakeYaml approach.
 */
class GpuWorkerProps(
    @BeanProperty var memory: String,
    @BeanProperty var count: Int,
    private var name: String) extends ValidatableProperties {

  var device: Option[GpuDevice] = None

  def this() = {
    this("0m", 0, "")
  }

  /**
   * Define custom getter for GPU name.
   */
  def getName: String = name

  /**
   * Define custom setter for GPU name to ensure it is always in lower case.
   *
   * @see [[com.nvidia.spark.rapids.tool.GpuTypes]]
   */
  def setName(newName: String): Unit = {
    this.name = newName.toLowerCase
  }

  override def validate(): Unit = {
    if (getName != null && getName.nonEmpty) {
      device = GpuDevice.createInstance(getName).orElse {
        val supportedGpus = GpuDevice.deviceMap.keys.mkString(", ")
        throw new IllegalArgumentException(
          s"Unsupported GPU type provided: $getName. Supported GPU types: $supportedGpus")
      }
    }
  }

  def isMissingInfo: Boolean = {
    memory == null || memory.isEmpty || name == null || name.isEmpty ||
       count == 0 || memory.startsWith("0") || name == "None"
  }
  def isEmpty: Boolean = {
    count == 0 && (memory == null || memory.isEmpty || memory.startsWith("0")) &&
      (name == null || name.isEmpty || name == "None")
  }
  /**
   * If the GPU count is missing, it will set 1 as a default value
   *
   * @return true if the value has been updated.
   */
  def setDefaultGpuCountIfMissing(tuningConfigs: TuningConfigsProvider): Boolean = {
    // TODO - do we want to recommend 1 or base it on core count?  32 cores to 1 gpu may be to much.
    if (count == 0) {
      count = tuningConfigs.getEntry("WORKER_GPU_COUNT").getDefault.toInt
      true
    } else {
      false
    }
  }
  def setDefaultGpuNameIfMissing(platform: Platform): Boolean = {
    if (!GpuDevice.deviceMap.contains(name)) {
      name = platform.gpuDevice.getOrElse(platform.defaultGpuDevice).toString
      true
    } else {
      false
    }
  }

  /**
   * If the GPU memory is missing, it will sets a default valued based on the GPU device type.
   * If it is still missing, it sets a default to 15109m (T4).
   *
   * @return true if the value has been updated.
   */
  def setDefaultGpuMemIfMissing(): Boolean = {
    if (memory == null || memory.isEmpty || memory.startsWith("0")) {
      memory = try {
        GpuDevice.createInstance(getName).getOrElse(GpuDevice.DEFAULT).getMemory
      } catch {
        case _: IllegalArgumentException => GpuDevice.DEFAULT.getMemory
      }
      true
    } else {
      false
    }
  }

  /**
   * Sets any missing field and return a list of messages to indicate what has been updated.
   * @return a list containing information of what was missing and the default value that has been
   *         used to initialize the field.
   */
  def setMissingFields(platform: Platform,
      tuningConfigs: TuningConfigsProvider): Seq[String] = {
    val res = new mutable.ListBuffer[String]()
    if (setDefaultGpuCountIfMissing(tuningConfigs)) {
      res += s"GPU count is missing. Setting default to $getCount."
    }
    if (setDefaultGpuNameIfMissing(platform)) {
      res += s"GPU device is missing. Setting default to $getName."
    }
    if (setDefaultGpuMemIfMissing()) {
      res += s"GPU memory is missing. Setting default to $getMemory."
    }
    res
  }

  override def toString: String =
    s"{count: $count, memory: $memory, name: $name}"
}

/**
 * A wrapper class that stores all the system properties.
 * The BeanProperty enables loading and parsing the YAML formatted content using the
 * Constructor SnakeYaml approach.
 */
class SystemClusterProps(
    @BeanProperty var numCores: Int,
    @BeanProperty var memory: String,
    @BeanProperty var numWorkers: Int) {
  def this() = {
    this(0, "0m", 0)
  }
  def isMissingInfo: Boolean = {
    // keep for future expansion as we may add more fields later.
    numWorkers <= 0
  }
  def isEmpty: Boolean = {
    // consider the object incorrect if either numCores or memory are not set.
    memory == null || memory.isEmpty || numCores <= 0 || memory.startsWith("0")
  }
  private def setDefaultNumWorkersIfMissing(tuningConfigs: TuningConfigsProvider)
  : Boolean = {
    if (numWorkers <= 0) {
      numWorkers = tuningConfigs.getEntry("NUM_WORKERS").getMin.toInt
      true
    } else {
      false
    }
  }
  /**
   * Sets any missing field and return a list of messages to indicate what has been updated.
   * @return a list containing information of what was missing and the default value that has been
   *         used to initialize the field.
   */
  def setMissingFields(tuningConfigs: TuningConfigsProvider): Seq[String] = {
    val res = new mutable.ListBuffer[String]()
    if (setDefaultNumWorkersIfMissing(tuningConfigs)) {
      res += s"Number of workers is missing. Setting default to $getNumWorkers."
    }
    res
  }
  override def toString: String =
    s"{numCores: $numCores, memory: $memory, numWorkers: $numWorkers}"
}

/**
 * A wrapper class that stores all the properties of the cluster.
 * The BeanProperty enables loading and parsing the YAML formatted content using the
 * Constructor SnakeYaml approach.
 *
 * @param system wrapper that includes the properties related to system information like cores and
 *               memory.
 * @param gpu wrapper that includes the properties related to GPU.
 * @param softwareProperties a set of software properties such as Spark properties.
 *                           The properties are typically loaded from the default cluster
 *                           configurations.
 */
class ClusterProperties(
    @BeanProperty var system: SystemClusterProps,
    @BeanProperty var gpu: GpuWorkerProps,
    @BeanProperty var softwareProperties: util.LinkedHashMap[String, String])
  extends ValidatableProperties {

  def this() = {
    this(new SystemClusterProps(), new GpuWorkerProps(), new util.LinkedHashMap[String, String]())
  }

  override def validate(): Unit = {
    gpu.validate()
  }

  def isEmpty: Boolean = {
    system.isEmpty && gpu.isEmpty
  }
  override def toString: String =
    s"{${system.toString}, ${gpu.toString}, $softwareProperties}"
}

/**
 * Represents different Spark master types.
 */
sealed trait SparkMaster {
  // Default executor memory to use in case not set by the user.
  val defaultExecutorMemoryMB: Long
}
case object Local extends SparkMaster {
  val defaultExecutorMemoryMB: Long = 0L
}
case object Yarn extends SparkMaster {
  val defaultExecutorMemoryMB: Long = 1024L
}
case object Kubernetes extends SparkMaster {
  val defaultExecutorMemoryMB: Long = 1024L
}
case object Standalone extends SparkMaster {
  // Would be the entire node memory by default
  val defaultExecutorMemoryMB: Long = 0L
}

object SparkMaster {
  def apply(master: Option[String]): Option[SparkMaster] = {
    master.flatMap {
      case url if url.contains("yarn") => Some(Yarn)
      case url if url.contains("k8s") => Some(Kubernetes)
      case url if url.contains("local") => Some(Local)
      case url if url.contains("spark://") => Some(Standalone)
      case _ => None
    }
  }
}

/**
 * AutoTuner module that uses event logs and worker's system properties to recommend Spark
 * RAPIDS configuration based on heuristics.
 *
 * Example:
 * a. Success:
 *    Input:
 *      system:
 *        num_cores: 64
 *        cpu_arch: x86_64
 *        memory: 512gb
 *        free_disk_space: 800gb
 *        time_zone: America/Los_Angeles
 *        num_workers: 4
 *      gpu:
 *        count: 8
 *        memory: 32gb
 *        name: NVIDIA V100
 *      softwareProperties:
 *        spark.driver.maxResultSize: 7680m
 *        spark.driver.memory: 15360m
 *        spark.executor.cores: '8'
 *        spark.executor.instances: '2'
 *        spark.executor.memory: 47222m
 *        spark.executorEnv.OPENBLAS_NUM_THREADS: '1'
 *        spark.extraListeners: com.google.cloud.spark.performance.DataprocMetricsListener
 *        spark.scheduler.mode: FAIR
 *        spark.sql.cbo.enabled: 'true'
 *        spark.ui.port: '0'
 *        spark.yarn.am.memory: 640m
 *
 *    Output:
 *       Spark Properties:
 *       --conf spark.executor.cores=8
 *       --conf spark.executor.instances=20
 *       --conf spark.executor.memory=16384m
 *       --conf spark.executor.memoryOverhead=5734m
 *       --conf spark.rapids.memory.pinnedPool.size=4096m
 *       --conf spark.rapids.sql.concurrentGpuTasks=2
 *       --conf spark.sql.files.maxPartitionBytes=4096m
 *       --conf spark.task.resource.gpu.amount=0.125
 *
 *       Comments:
 *       - 'spark.rapids.sql.concurrentGpuTasks' was not set.
 *       - 'spark.executor.memoryOverhead' was not set.
 *       - 'spark.rapids.memory.pinnedPool.size' was not set.
 *       - 'spark.sql.adaptive.enabled' should be enabled for better performance.
 *
 * b. Failure:
 *    Input: Incorrect File
 *    Output:
 *      Cannot recommend properties. See Comments.
 *
 *      Comments:
 *      - java.io.FileNotFoundException: File worker_info.yaml does not exist
 *      - 'spark.executor.memory' should be set to at least 2GB/core.
 *      - 'spark.executor.instances' should be set to (gpuCount * numWorkers).
 *      - 'spark.task.resource.gpu.amount' should be set to Max(1, (numCores / gpuCount)).
 *      - 'spark.rapids.sql.concurrentGpuTasks' should be set to Min(4, (gpuMemory / 7.5G)).
 *      - 'spark.rapids.memory.pinnedPool.size' should be set to 2048m.
 *      - 'spark.sql.adaptive.enabled' should be enabled for better performance.
 *
 * @param clusterProps The cluster properties including cores, mem, GPU, and software
 *                     (see [[ClusterProperties]]).
 * @param appInfoProvider the container holding the profiling result.
 */
abstract class AutoTuner(
    val clusterProps: ClusterProperties,
    val appInfoProvider: AppSummaryInfoBaseProvider,
    val platform: Platform,
    val driverInfoProvider: DriverLogInfoProvider,
    val userProvidedTuningConfigs: Option[TuningConfigsProvider],
    val autoTunerHelper: AutoTunerHelper)
  extends Logging with AutoTunerCommentsWithTuningConfigs with AutoTunerStaticComments {

  lazy val tuningConfigs: TuningConfigsProvider = {
    // Load the default configs from the resource file (including tool-specific overrides).
    val baseConfigs = PropertiesLoader[TuningConfigsProvider].loadFromContent(
        UTF8Source.fromResource(TuningConfigsProvider.DEFAULT_CONFIGS_FILE).mkString
      ).getOrElse {
        throw new RuntimeException(
          "Failed to load default value for tuning config: " +
            "The file is missing or has invalid format.")
      }.withAutoTuner(Some(this))
    // Merge with user provided configs if any.
    userProvidedTuningConfigs.map(baseConfigs.merge).getOrElse(baseConfigs)
  }

  var comments = new mutable.ListBuffer[String]()
  var recommendations: mutable.LinkedHashMap[String, TuningEntryTrait] =
    mutable.LinkedHashMap[String, TuningEntryTrait]()
  // list of recommendations to be skipped for recommendations
  // Note that the recommendations will be computed anyway to avoid breaking dependencies.
  private val skippedRecommendations: mutable.HashSet[String] = mutable.HashSet[String]()
  // list of recommendations having the calculations disabled, and only depend on default values
  protected val limitedLogicRecommendations: mutable.HashSet[String] = mutable.HashSet[String]()
  // When enabled, the profiler recommendations should only include updated settings.
  private var filterByUpdatedPropertiesEnabled: Boolean = true

  private lazy val sparkMaster: Option[SparkMaster] = {
    SparkMaster(appInfoProvider.getProperty("spark.master"))
  }

  private def isCalculationEnabled(prop: String) : Boolean = {
    !limitedLogicRecommendations.contains(prop)
  }

  /**
   * Used to get the property value from the source properties
   * (i.e. from app info and cluster properties)
   */
  private def getPropertyValueFromSource(key: String): Option[String] = {
    getAllSourceProperties.get(key)
  }

  /**
   * Used to get the property value in the following priority order:
   * 1. Recommendations (this also includes the user-enforced properties)
   * 2. Source Spark properties (i.e. from app info and cluster properties)
   */
  protected def getPropertyValue(key: String): Option[String] = {
    AutoTuner.getCombinedPropertyFn(recommendations, getAllSourceProperties)(key)
  }

  /**
   * Get combined properties from the app info and cluster properties.
   */
  private lazy val getAllSourceProperties: Map[String, String] = {
    // the cluster properties override the app properties as
    // it is provided by the user.
    appInfoProvider.getAllProperties ++ clusterProps.getSoftwareProperties.asScala
  }

  /**
   * Combined tuning table that merges the default tuning definitions with user-defined ones.
   */
  private lazy val finalTuningTable = TuningEntryDefinition.TUNING_TABLE ++
    platform.targetCluster
      .map(_.getSparkProperties.tuningDefinitionsMap)
      .getOrElse(Map.empty[String, TuningEntryDefinition])

  def initRecommendations(): Unit = {
    finalTuningTable.keys.foreach { key =>
      // no need to add new records if they are missing from props
      getPropertyValueFromSource(key).foreach { propVal =>
        val recommendationVal = TuningEntry.build(key, Option(propVal), None,
          finalTuningTable.get(key))
        recommendations(key) = recommendationVal
      }
    }
    // Add the enforced properties to the recommendations.
    platform.userEnforcedRecommendations.foreach {
      case (key, value) =>
        val recomRecord = recommendations.getOrElseUpdate(key,
          TuningEntry.build(key, getPropertyValueFromSource(key), None, finalTuningTable.get(key)))
        recomRecord.setRecommendedValue(value)
        appendComment(getEnforcedPropertyComment(key))
    }
  }

  /**
   * Append a comment to the list by looking up the persistent comment if any in the tuningEntry
   * table.
   * @param key the property set by the autotuner.
   */
  private def appendMissingComment(key: String): Unit = {
    val missingComment = finalTuningTable.get(key)
      .flatMap(_.getMissingComment())
      .getOrElse(s"was not set.")
    appendComment(s"'$key' $missingComment")
  }

  /**
   * Append a comment to the list by looking up the persistent comment if any.
   * @param key the property set by the autotuner.
   */
  private def appendPersistentComment(key: String): Unit = {
    finalTuningTable.get(key).foreach { eDef =>
      eDef.getPersistentComment().foreach { comment =>
        appendComment(s"'$key' $comment")
      }
    }
  }

  /**
   * Append a comment to the list by looking up the updated comment if any in the tuningEntry
   * table. If it is not defined in the table, then add nothing.
   * @param key the property set by the autotuner.
   */
  private def appendUpdatedComment(key: String): Unit = {
    finalTuningTable.get(key).foreach { eDef =>
      eDef.getUpdatedComment().foreach { comment =>
        appendComment(s"'$key' $comment")
      }
    }
  }

  def appendRecommendation(key: String, value: String): Unit = {
    if (skippedRecommendations.contains(key)) {
      // do not do anything if the recommendations should be skipped
      return
    }
    if (platform.getUserEnforcedSparkProperty(key).isDefined) {
      // If the property is enforced by the user, the recommendation should be
      // skipped as we have already added it during the initialization.
      return
    }
    // Update the recommendation entry or update the existing one.
    val recomRecord = recommendations.getOrElseUpdate(key,
      TuningEntry.build(key, getPropertyValue(key), None, finalTuningTable.get(key)))
    // if the value is not null, then proceed to add the recommendation.
    Option(value).foreach { nonNullValue =>
      recomRecord.setRecommendedValue(nonNullValue)
      recomRecord.getOriginalValue match {
        case None =>
          // add missing comment if any
          appendMissingComment(key)
        case Some(originalValue) if originalValue != recomRecord.getTuneValue() =>
          // add updated comment if any
          appendUpdatedComment(key)
        case _ =>
          // do not add any comment if the tuned value is the same as the original value
      }
      // add the persistent comment if any.
      appendPersistentComment(key)
    }
  }

  /**
   * Safely appends the recommendation to the given key.
   * It skips if the value is 0.
   */
  def appendRecommendation(key: String, value: Int): Unit = {
    if (value > 0) {
      appendRecommendation(key: String, s"$value")
    }
  }

  /**
   * Safely appends the recommendation to the given key.
   * It skips if the value is 0.0.
   */
  def appendRecommendation(key: String, value: Double): Unit = {
    if (value > 0.0) {
      appendRecommendation(key: String, s"$value")
    }
  }
  /**
   * Safely appends the recommendation to the given key.
   * It appends "m" to the string value. It skips if the value is 0 or null.
   */
  def appendRecommendationForMemoryMB(key: String, value: String): Unit = {
    if (value != null && value.toDouble > 0.0) {
      appendRecommendation(key, s"${value}m")
    }
  }

  /**
   * Try to figure out the recommended instance type to use and set
   * the executor cores and instances based on that instance type.
   * Returns None if the platform doesn't support specific instance types.
   */
  private def configureGPURecommendedInstanceType(): Unit = {
    platform.createRecommendedGpuClusterInfo(recommendations, getAllSourceProperties,
      autoTunerHelper.recommendedClusterSizingStrategy)
    platform.recommendedClusterInfo.foreach { gpuClusterRec =>
      // TODO: Should we skip recommendation if cores per executor is lower than a min value?
      appendRecommendation("spark.executor.cores", gpuClusterRec.coresPerExecutor)
      if (gpuClusterRec.numExecutors > 0) {
        appendRecommendation("spark.executor.instances", gpuClusterRec.numExecutors)
      }
    }
  }

  def calcNumExecutorCores: Int = {
    val executorCores = platform.recommendedClusterInfo.map(_.coresPerExecutor).getOrElse(1)
    Math.max(1, executorCores)
  }

  /**
   * Recommendation for 'spark.rapids.sql.concurrentGpuTasks' based on gpu memory.
   * Assumption - cluster properties were updated to have a default values if missing.
   */
  private def calcGpuConcTasks(): Long = {
    Math.min(tuningConfigs.getEntry("CONC_GPU_TASKS").getMax.toLong,
      platform.recommendedGpuDevice.getGpuConcTasks(
        tuningConfigs.getEntry("GPU_MEM_PER_TASK").getDefaultAsMemory(ByteUnit.MiB)))
  }

  /**
   * Recommendation for initial heap size based on certain amount of memory per core.
   * Note that we will later reduce this if needed for off heap memory.
   */
  def calcInitialExecutorHeapInMB(executorContainerMemCalculator: () => Double,
      numExecCores: Int): Long = {
    val maxExecutorHeap = Math.max(0, executorContainerMemCalculator()).toInt
    // give up to 2GB of heap to each executor core
    // TODO - revisit this in future as we could let heap be bigger
    Math.min(maxExecutorHeap,
      tuningConfigs.getEntry("HEAP_PER_CORE").getDefaultAsMemory(ByteUnit.MiB) * numExecCores)
  }

  /**
   * Recommendation for maxBytesInFlight.
   *
   * TODO: To be removed in the future https://github.com/NVIDIA/spark-rapids-tools/issues/1710
   */
  private lazy val recommendedMaxBytesInFlightMB: Long = {
    val valueStr =
      platform.getUserEnforcedSparkProperty("spark.rapids.shuffle.multiThreaded.maxBytesInFlight")
      .getOrElse(tuningConfigs.getEntry("MAX_BYTES_IN_FLIGHT").getDefault)
    StringUtils.convertToMB(valueStr, Some(ByteUnit.BYTE))
  }

  /**
   * Note: All memory values are in MB.
   */
  private case class MemorySettings(
    executorHeap: Option[Long],
    executorMemOverhead: Option[Long],
    pinnedMem: Option[Long],
    spillMem: Option[Long]
  ) {
    def hasAnyMemorySettings: Boolean = {
      executorMemOverhead.isDefined || pinnedMem.isDefined || spillMem.isDefined
    }
  }

  private lazy val userEnforcedMemorySettings: MemorySettings = {
    val executorHeap = platform.getUserEnforcedSparkProperty("spark.executor.memory")
      .map(StringUtils.convertToMB(_, Some(ByteUnit.BYTE)))
    val executorMemOverhead = platform.getUserEnforcedSparkProperty("spark.executor.memoryOverhead")
      .map(StringUtils.convertToMB(_, Some(ByteUnit.BYTE)))
    val pinnedMem = platform.getUserEnforcedSparkProperty("spark.rapids.memory.pinnedPool.size")
      .map(StringUtils.convertToMB(_, Some(ByteUnit.BYTE)))
    val spillMem = platform.getUserEnforcedSparkProperty("spark.rapids.memory.spillPool.size")
      .map(StringUtils.convertToMB(_, Some(ByteUnit.BYTE)))
    MemorySettings(executorHeap, executorMemOverhead, pinnedMem, spillMem)
  }

  private def generateInsufficientMemoryComment(
      executorHeap: Long,
      finalExecutorMemOverhead: Long,
      sparkOffHeapMemMB: Long,
      pySparkMemMB: Long): String = {
    val minTotalExecMemRequired = (
      // Calculate total system memory needed by dividing executor memory by usable fraction.
      // Accounts for memory reserved by the container manager (e.g., YARN).
      (executorHeap + finalExecutorMemOverhead + sparkOffHeapMemMB + pySparkMemMB) /
        platform.fractionOfSystemMemoryForExecutors
      ).toLong
    notEnoughMemComment(minTotalExecMemRequired)
  }

  // scalastyle:off line.size.limit
  /**
   * Calculates recommended memory settings for a Spark executor container.
   *
   * The total memory for the executor is the sum of:
   *   executorHeap (spark.executor.memory)
   *   + executorMemOverhead (spark.executor.memoryOverhead)
   *   + sparkOffHeapMemMB (spark.memory.offHeap.size)
   *   + pySparkMemMB (spark.executor.pyspark.memory)
   *
   * Note: In the below examples, `0.8` is the fraction of the physical system memory
   * that is available to Spark executors (0.2 is reserved by Dataproc YARN).
   *
   * Example 1: g2-standard-8 machine (32 GB total memory) — Just enough memory
   *   - actualMemForExec =  32 GB * 0.8 = 25.6 GB
   *   - executorHeap = 16 GB
   *   - sparkOffHeapMemMB = 4 GB
   *   - execMemLeft = 25.6 GB - 16 GB - 4 GB = 5.6 GB
   *   - minOverhead = 1.6 GB (10% of executor heap) + 2 GB (min pinned) + 2 GB (min spill) = 5.6 GB
   *   - Since execMemLeft (5.6 GB) == minOverhead (5.6 GB), proceed with minimum memory recommendations:
   *   - Recommendation:
   *       - executorHeap = 16 GB, executorMemOverhead = 5.6 GB (with pinnedMem = 2 GB and spillMem = 2 GB)
   *
   * Example 2: g2-standard-16 machine (64 GB total memory) — Not enough memory
   *   - actualMemForExec = 64 GB * 0.8 = 51.2 GB
   *   - executorHeap = 32 GB
   *   - sparkOffHeapMemMB = 20 GB
   *   - execMemLeft = 51.2 GB - 32 GB - 20 GB = -0.8 GB
   *   - minOverhead = 2 GB (min pinned) + 2 GB (min spill) + 3.2 GB (10% of executor heap) = 7.2 GB
   *   - Since execMemLeft (-0.8 GB) < minOverhead (7.2 GB), do not proceed with recommendations
   *       - Add a warning comment indicating that the current setup is not optimal
   *           - minTotalExecMemRequired = (32 GB + 20 GB + 7.2 GB) / 0.8 = (59.2 GB / 0.8) = 74 GB (as we are using 80% of system memory)
   *           - Reduce off-heap size or use a larger machine with at least 74 GB system memory.
   *
   * Example 3: g2-standard-16 machine (64 GB total memory) — More memory available
   *   - actualMemForExec = 64 GB * 0.8 = 51.2 GB
   *   - executorHeap = 32 GB
   *   - sparkOffHeapMemMB = 10 GB
   *   - execMemLeft = 51.2 GB - 32 GB - 10 GB = 9.2 GB
   *   - minOverhead = 2 GB (min pinned) + 2 GB (min spill) + 3.2 GB (10% of executor heap) = 7.2 GB
   *   - Since execMemLeft (9.2 GB) > minOverhead (7.2 GB), proceed with recommendations.
   *       - Increase pinned and spill memory based on remaining memory (up to 4 GB max)
   *       - executorMemOverhead = 3 GB (pinned) + 3 GB (spill) + 3.2 GB = 9.2 GB
   *   - Recommendation:
   *       - executorHeap = 32 GB, executorMemOverhead = 9.2 GB (with pinnedMem = 3 GB and spillMem = 3 GB)
   *
   *
   * @param execHeapCalculator    Function that returns the executor heap size in MB
   * @param numExecutorCores      Number of executor cores
   * @param totalMemForExecExpr   Function that returns total memory available to the executor (MB)
   * @return Either a String with an error message if memory is insufficient,
   *         or a tuple containing:
   *           - pinned memory size (MB)
   *           - executor memory overhead size (MB)
   *           - executor heap size (MB)
   *           - boolean indicating if "maxBytesInFlight" should be set
   */
   // scalastyle:on line.size.limit
  private def calcOverallMemory(
      execHeapCalculator: () => Long,
      numExecutorCores: Int,
      totalMemForExecExpr: () => Double): Either[String, (MemorySettings, Boolean)] = {

    // Set executor heap using user enforced value or max of calculator result and 2GB/core
    val executorHeapMB = userEnforcedMemorySettings.executorHeap.getOrElse {
      Math.max(execHeapCalculator(),
        tuningConfigs.getEntry("HEAP_PER_CORE").getDefaultAsMemory(ByteUnit.MiB) * numExecutorCores)
    }
    // Our CSP instance map stores full node memory, but container managers
    // (e.g., YARN) may reserve a portion. Adjust to get the memory
    // actually available to the executor.
    val actualMemForExec = {
      totalMemForExecExpr.apply() * platform.fractionOfSystemMemoryForExecutors
    }.toLong
    // Get a combined spark properties function that includes user enforced properties
    // and properties from the event log
    val sparkPropertiesFn = AutoTuner.getCombinedPropertyFn(recommendations, getAllSourceProperties)
    val sparkOffHeapMemMB = platform.getSparkOffHeapMemoryMB(sparkPropertiesFn).getOrElse(0L)
    val pySparkMemMB = platform.getPySparkMemoryMB(sparkPropertiesFn).getOrElse(0L)
    val execMemLeft = actualMemForExec - executorHeapMB - sparkOffHeapMemMB - pySparkMemMB
    var setMaxBytesInFlight = false
    // reserve 10% of heap as memory overhead
    var executorMemOverhead = (
      executorHeapMB * tuningConfigs.getEntry("HEAP_OVERHEAD_FRACTION").getDefault.toDouble
    ).toLong
    val defaultPinnedMem = tuningConfigs.getEntry("PINNED_MEMORY").getDefaultAsMemory(ByteUnit.MiB)
    val defaultSpillMem = tuningConfigs.getEntry("SPILL_MEMORY").getDefaultAsMemory(ByteUnit.MiB)

    val minOverhead = userEnforcedMemorySettings.executorMemOverhead.getOrElse {
      executorMemOverhead + defaultPinnedMem + defaultSpillMem
    }
    logDebug(s"Memory calculations:  actualMemForExec=$actualMemForExec MB, " +
      s"executorHeap=$executorHeapMB MB, sparkOffHeapMem=$sparkOffHeapMemMB MB, " +
      s"pySparkMem=$pySparkMemMB MB minOverhead=$minOverhead MB")
    if (execMemLeft >= minOverhead) {
      // this is hopefully path in the majority of cases because CSPs generally have a good
      // memory to core ratio
      // Account for the setting of `maxBytesInFlight`
      if (numExecutorCores >= 16 && platform.isPlatformCSP &&
        execMemLeft >
          executorMemOverhead + recommendedMaxBytesInFlightMB +
            defaultPinnedMem + defaultSpillMem) {
        executorMemOverhead += recommendedMaxBytesInFlightMB
        setMaxBytesInFlight = true
      }
      // Pinned memory uses any unused space up to 4GB. Spill memory is same size as pinned.
      var pinnedMem = userEnforcedMemorySettings.pinnedMem.getOrElse {
        Math.min(tuningConfigs.getEntry("PINNED_MEMORY").getMaxAsMemory(ByteUnit.MiB),
          (execMemLeft - executorMemOverhead) / 2)
      }
      // Spill storage is set to the pinned size by default. Its not guaranteed to use just pinned
      // memory though so the size worst case would be doesn't use any pinned memory and uses
      // all off heap memory.
      var spillMem = userEnforcedMemorySettings.spillMem.getOrElse(pinnedMem)
      var finalExecutorMemOverhead = userEnforcedMemorySettings.executorMemOverhead.getOrElse {
        executorMemOverhead + pinnedMem + spillMem
      }
      // Handle the case when the final executor memory overhead is larger than the
      // available memory left for the executor.
      if (execMemLeft < finalExecutorMemOverhead) {
        // If there is any user-enforced memory settings, add a warning comment
        // indicating that the current setup is not optimal and no memory-related
        // tunings are recommended.
        if (userEnforcedMemorySettings.hasAnyMemorySettings) {
          return Left(generateInsufficientMemoryComment(executorHeapMB, finalExecutorMemOverhead,
            sparkOffHeapMemMB, pySparkMemMB))
        }
        // Else update pinned and spill memory to use default values
        pinnedMem = defaultPinnedMem
        spillMem = defaultSpillMem
        finalExecutorMemOverhead = executorMemOverhead + defaultPinnedMem + defaultSpillMem
      }
      // Add recommendations for executor memory settings and a boolean for maxBytesInFlight
      Right((MemorySettings(Some(executorHeapMB), Some(finalExecutorMemOverhead), Some(pinnedMem),
        Some(spillMem)), setMaxBytesInFlight))
    } else {
      // Add a warning comment indicating that the current setup is not optimal
      // and no memory-related tunings are recommended.
      // TODO: For CSPs, we should recommend a different instance type.
      Left(generateInsufficientMemoryComment(executorHeapMB, minOverhead,
        sparkOffHeapMemMB, pySparkMemMB))
    }
  }

  private def configureShuffleReaderWriterNumThreads(numExecutorCores: Int): Unit = {
    // if on a CSP using blob store recommend more threads for certain sizes. This is based on
    // testing on customer jobs on Databricks
    // didn't test with > 16 thread so leave those as numExecutorCores
    if (numExecutorCores < 4) {
      // leave as defaults - should we reduce less then default of 20? need more testing
    } else if (numExecutorCores >= 4 && numExecutorCores < 16) {
      appendRecommendation("spark.rapids.shuffle.multiThreaded.reader.threads", 20)
      appendRecommendation("spark.rapids.shuffle.multiThreaded.writer.threads", 20)
    } else if (numExecutorCores >= 16 && numExecutorCores < 20 && platform.isPlatformCSP) {
      appendRecommendation("spark.rapids.shuffle.multiThreaded.reader.threads", 28)
      appendRecommendation("spark.rapids.shuffle.multiThreaded.writer.threads", 28)
    } else {
      val numThreads = (numExecutorCores * 1.5).toLong
      appendRecommendation("spark.rapids.shuffle.multiThreaded.reader.threads", numThreads.toInt)
      appendRecommendation("spark.rapids.shuffle.multiThreaded.writer.threads", numThreads.toInt)
    }
  }

  // Currently only applies many configs for CSPs where we have an idea what network/disk
  // configuration is like. On prem we don't know so don't set these for now.
  private def configureMultiThreadedReaders(numExecutorCores: Int,
      setMaxBytesInFlight: Boolean): Unit = {
    if (numExecutorCores < 4) {
      appendRecommendation("spark.rapids.sql.multiThreadedRead.numThreads",
        Math.max(20, numExecutorCores))
    } else if (numExecutorCores >= 4 && numExecutorCores < 8 && platform.isPlatformCSP) {
      appendRecommendation("spark.rapids.sql.multiThreadedRead.numThreads",
        Math.max(20, numExecutorCores))
    } else if (numExecutorCores >= 8 && numExecutorCores < 16 && platform.isPlatformCSP) {
      appendRecommendation("spark.rapids.sql.multiThreadedRead.numThreads",
        Math.max(40, numExecutorCores))
    } else if (numExecutorCores >= 16 && numExecutorCores < 20 && platform.isPlatformCSP) {
      appendRecommendation("spark.rapids.sql.multiThreadedRead.numThreads",
        Math.max(80, numExecutorCores))
      if (setMaxBytesInFlight) {
        appendRecommendationForMemoryMB("spark.rapids.shuffle.multiThreaded.maxBytesInFlight",
          recommendedMaxBytesInFlightMB.toString)
      }
      appendRecommendation("spark.rapids.sql.reader.multithreaded.combine.sizeBytes",
        tuningConfigs.getEntry("READER_MULTITHREADED_COMBINE_THRESHOLD").getDefault)
      appendRecommendation("spark.rapids.sql.format.parquet.multithreaded.combine.waitTime",
        tuningConfigs.getEntry("READER_MULTITHREADED_COMBINE_WAIT_TIME").getDefault)
    } else {
      val numThreads = numExecutorCores * tuningConfigs
        .getEntry("MULTITHREAD_READ_CORE_MULTIPLIER").getDefault.toInt
      appendRecommendation("spark.rapids.sql.multiThreadedRead.numThreads",
        Math.max(tuningConfigs.getEntry("MULTITHREAD_READ_NUM_THREADS").getMax.toInt, numThreads))
      if (platform.isPlatformCSP) {
        if (setMaxBytesInFlight) {
          appendRecommendationForMemoryMB("spark.rapids.shuffle.multiThreaded.maxBytesInFlight",
            recommendedMaxBytesInFlightMB.toString)
        }
        appendRecommendation("spark.rapids.sql.reader.multithreaded.combine.sizeBytes",
          tuningConfigs.getEntry("READER_MULTITHREADED_COMBINE_THRESHOLD").getDefault)
        appendRecommendation("spark.rapids.sql.format.parquet.multithreaded.combine.waitTime",
          tuningConfigs.getEntry("READER_MULTITHREADED_COMBINE_WAIT_TIME").getDefault)
      }
    }
  }


  def calculateClusterLevelRecommendations(): Unit = {
    // only if we were able to figure out a node type to recommend do we make
    // specific recommendations
    if (platform.recommendedClusterInfo.isDefined) {
      val execCores = platform.recommendedClusterInfo.map(_.coresPerExecutor).getOrElse(1)
      // Set to low value for Spark RAPIDS usage as task parallelism will be honoured
      // by `spark.executor.cores`.
      appendRecommendation("spark.task.resource.gpu.amount",
        tuningConfigs.getEntry("TASK_GPU_RESOURCE_AMT").getDefault.toDouble)
      appendRecommendation("spark.rapids.sql.concurrentGpuTasks",
        calcGpuConcTasks().toInt)
      val availableMemPerExec =
        platform.recommendedWorkerNode.map(_.getMemoryPerExec).getOrElse(0.0)
      val shouldSetMaxBytesInFlight = if (availableMemPerExec > 0.0) {
        val availableMemPerExecExpr = () => availableMemPerExec
        val executorHeapInMB = calcInitialExecutorHeapInMB(availableMemPerExecExpr, execCores)
        val executorHeapExpr = () => executorHeapInMB
        calcOverallMemory(executorHeapExpr, execCores, availableMemPerExecExpr) match {
          case Right((recomMemorySettings: MemorySettings, setMaxBytesInFlight)) =>
            // Sufficient memory available, proceed with recommendations
            appendRecommendationForMemoryMB("spark.rapids.memory.pinnedPool.size",
              s"${recomMemorySettings.pinnedMem.get}")
            // scalastyle:off line.size.limit
            // For YARN and Kubernetes, we need to set the executor memory overhead
            // Ref: https://spark.apache.org/docs/latest/configuration.html#:~:text=This%20option%20is%20currently%20supported%20on%20YARN%20and%20Kubernetes.
            // scalastyle:on line.size.limit
            if (sparkMaster.contains(Yarn) || sparkMaster.contains(Kubernetes)) {
              appendRecommendationForMemoryMB("spark.executor.memoryOverhead",
                s"${recomMemorySettings.executorMemOverhead.get}")
            }
            appendRecommendationForMemoryMB("spark.executor.memory",
              s"${recomMemorySettings.executorHeap.get}")
            setMaxBytesInFlight
          case Left(notEnoughMemComment) =>
            // Not enough memory available, add warning comments
            appendComment(notEnoughMemComment)
            appendComment("spark.rapids.memory.pinnedPool.size",
              notEnoughMemCommentForKey(
                "spark.rapids.memory.pinnedPool.size"))
            if (sparkMaster.contains(Yarn) || sparkMaster.contains(Kubernetes)) {
              appendComment("spark.executor.memoryOverhead",
                notEnoughMemCommentForKey(
                  "spark.executor.memoryOverhead"))
            }
            appendComment("spark.executor.memory",
              notEnoughMemCommentForKey(
                "spark.executor.memory"))
            false
        }
      } else {
        logInfo("Available memory per exec is not specified")
        addMissingMemoryComments()
        false
      }
      configureShuffleReaderWriterNumThreads(execCores)
      configureMultiThreadedReaders(execCores, shouldSetMaxBytesInFlight)
      // TODO: Should we recommend AQE even if cluster properties are not enabled?
      recommendAQEProperties()
    } else {
      addDefaultComments()
    }
    appendRecommendation("spark.rapids.sql.batchSizeBytes",
      tuningConfigs.getEntry("BATCH_SIZE_BYTES").getDefault)
    appendRecommendation("spark.locality.wait",
      tuningConfigs.getEntry("LOCALITY_WAIT").getDefault)
  }

  def calculateJobLevelRecommendations(): Unit = {
    // TODO - do we do anything with 200 shuffle partitions or maybe if its close
    // set the Spark config  spark.shuffle.sort.bypassMergeThreshold
    getShuffleManagerClassName match {
      case Right(smClassName) => appendRecommendation("spark.shuffle.manager", smClassName)
      case Left(comment) => appendComment("spark.shuffle.manager", comment)
    }
    appendComment(classPathComments("rapids.shuffle.jars"))
    recommendFileCache()
    recommendMaxPartitionBytes()
    recommendShufflePartitions()
    recommendKryoSerializerSetting()
    recommendGCProperty()
    if (platform.requirePathRecommendations) {
      recommendClassPathEntries()
    }
    recommendSystemProperties()
  }

  // if the user set the serializer to use Kryo, make sure we recommend using the GPU version
  // of it.
  def recommendKryoSerializerSetting(): Unit = {
    getPropertyValue("spark.serializer")
      .filter(_.contains("org.apache.spark.serializer.KryoSerializer")).foreach { _ =>
      // Logic:
      // - Trim whitespace, filter out empty entries and remove duplicates.
      // - Finally, append the GPU Kryo registrator to the existing set of registrators
      // Note:
      //  - ListSet preserves the original order of registrators
      // Example:
      //  property: "spark.kryo.registrator=reg1, reg2,, reg1"
      //  existingRegistrators: ListSet("reg1", "reg2")
      //  recommendation: "spark.kryo.registrator=reg1,reg2,GpuKryoRegistrator"
      val existingRegistrators = getPropertyValue("spark.kryo.registrator")
        .map(v => v.split(",").map(_.trim).filter(_.nonEmpty))
        .getOrElse(Array.empty)
        .to[scala.collection.immutable.ListSet]
      appendRecommendation("spark.kryo.registrator",
        (existingRegistrators + autoTunerHelper.gpuKryoRegistratorClassName).mkString(",")
      )
      // set the kryo serializer buffer size to prevent OOMs
      val desiredBufferMaxMB =
        tuningConfigs.getEntry("KRYO_SERIALIZER_BUFFER").getMaxAsMemory(ByteUnit.MiB)
      val currentBufferMaxMB = getPropertyValue("spark.kryoserializer.buffer.max")
        .map(StringUtils.convertToMB(_, Some(ByteUnit.MiB)))
        .getOrElse(0L)
      if (currentBufferMaxMB < desiredBufferMaxMB) {
        appendRecommendationForMemoryMB("spark.kryoserializer.buffer.max", s"$desiredBufferMaxMB")
      }
    }
  }

  /**
   * Resolves the RapidsShuffleManager class name based on the Spark version.
   * If a valid class name is not found, an error message is returned.
   *
   * Example:
   * sparkVersion: "3.2.0-amzn-1"
   * return: Right("com.nvidia.spark.rapids.spark320.RapidsShuffleManager")
   *
   * sparkVersion: "3.1.2"
   * return: Left("Cannot recommend RAPIDS Shuffle Manager for unsupported '3.1.2' version.")
   *
   * @return Either an error message (Left) or the RapidsShuffleManager class name (Right)
   */
  def getShuffleManagerClassName: Either[String, String] = {
    appInfoProvider.getSparkVersion match {
      case Some(sparkVersion) =>
        platform.getShuffleManagerVersion(sparkVersion) match {
          case Some(smVersion) =>
            Right(autoTunerHelper.buildShuffleManagerClassName(smVersion))
          case None =>
            Left(shuffleManagerCommentForUnsupportedVersion(
              sparkVersion, platform))
        }
      case None =>
        Left(shuffleManagerCommentForMissingVersion)
    }
  }

  /**
   * If the cluster worker-info is missing entries (i.e., CPU and GPU count), it sets the entries
   * to default values. For each default value, a comment is added to the [[comments]].
   */
  def configureClusterPropDefaults: Unit = {
    if (!clusterProps.system.isEmpty) {
      if (clusterProps.system.isMissingInfo) {
        clusterProps.system.setMissingFields(tuningConfigs)
          .foreach(m => appendComment(m))
      }
      if (clusterProps.gpu.isMissingInfo) {
        clusterProps.gpu.setMissingFields(platform, tuningConfigs)
          .foreach(m => appendComment(m))
      }
    }
  }

  private def recommendGCProperty(): Unit = {
    val jvmGCFraction = appInfoProvider.getJvmGCFractions
    if (jvmGCFraction.nonEmpty) { // avoid zero division
      if ((jvmGCFraction.sum / jvmGCFraction.size) >
        tuningConfigs.getEntry("JVM_GCTIME_FRACTION").getMax.toDouble) {
        // TODO - or other cores/memory ratio
        appendComment("Average JVM GC time is very high. " +
          "Other Garbage Collectors can be used for better performance.")
      }
    }
  }

  private def recommendAQEProperties(): Unit = {
    // Spark configuration (AQE is enabled by default)
    val aqeEnabled = getPropertyValue("spark.sql.adaptive.enabled")
      .getOrElse("false").toLowerCase
    if (aqeEnabled == "false") {
      // TODO: Should we recommend enabling AQE if not set?
      appendComment(commentsForMissingProps("spark.sql.adaptive.enabled"))
    }
    appInfoProvider.getSparkVersion match {
      case Some(version) =>
        if (ToolUtils.isSpark320OrLater(version)) {
          // AQE configs changed in 3.2.0
          if (getPropertyValue("spark.sql.adaptive.coalescePartitions.minPartitionSize").isEmpty) {
            appendRecommendation("spark.sql.adaptive.coalescePartitions.minPartitionSize",
              tuningConfigs.getEntry("AQE_MIN_PARTITION_SIZE").getDefault)
          }
        } else {
          if (getPropertyValue("spark.sql.adaptive.coalescePartitions.minPartitionNum").isEmpty) {
            // The ideal setting is for the parallelism of the cluster
            val numCoresPerExec = calcNumExecutorCores
            // TODO: Should this based on the recommended cluster instead of source cluster props?
            val numExecutorsPerWorker = clusterProps.gpu.getCount
            val numWorkers = clusterProps.system.getNumWorkers
            if (numExecutorsPerWorker != 0 && numWorkers != 0) {
              val total = numWorkers * numExecutorsPerWorker * numCoresPerExec
              appendRecommendation("spark.sql.adaptive.coalescePartitions.minPartitionNum",
                total.toString)
            }
          }
        }
      case None =>
    }

    val aqeInputSizeThresholdBytes = tuningConfigs.getEntry("AQE_INPUT_SIZE_THRESHOLD")
      .getDefaultAsMemory(ByteUnit.BYTE)
    val advisoryPartitionSizeProperty =
      getPropertyValue("spark.sql.adaptive.advisoryPartitionSizeInBytes")
    if (appInfoProvider.getMeanInput < aqeInputSizeThresholdBytes) {
      if (advisoryPartitionSizeProperty.isEmpty) {
        // get the default advisory partition size from the tuning config
        appendRecommendation("spark.sql.adaptive.advisoryPartitionSizeInBytes",
          tuningConfigs.getEntry("AQE_ADVISORY_PARTITION_SIZE").getDefault)
      }
    }
    var recInitialPartitionNum = 0
    val aqeShuffleReadBytesThresholdBytes = tuningConfigs.getEntry("AQE_SHUFFLE_READ_THRESHOLD")
      .getDefaultAsMemory(ByteUnit.BYTE)
    if (appInfoProvider.getMeanInput > aqeInputSizeThresholdBytes &&
      appInfoProvider.getMeanShuffleRead > aqeShuffleReadBytesThresholdBytes) {
      // AQE Recommendations for large input and large shuffle reads
      platform.recommendedGpuDevice.getAdvisoryPartitionSizeInBytes.foreach { size =>
        appendRecommendation("spark.sql.adaptive.advisoryPartitionSizeInBytes", size)
      }
      val initialPartitionNumValue = getInitialPartitionNumValue.map(_.toInt)
      if (initialPartitionNumValue.getOrElse(0) <=
            tuningConfigs.getEntry("AQE_MIN_INITIAL_PARTITION_NUM").getDefault.toInt) {
        recInitialPartitionNum = platform.recommendedGpuDevice.getInitialPartitionNum.getOrElse(0)
      }
      appendRecommendation("spark.sql.adaptive.coalescePartitions.parallelismFirst",
        tuningConfigs.getEntry("AQE_COALESCE_PARALLELISM_FIRST").getDefault)
    }

    val recShufflePartitions = recommendations.get("spark.sql.shuffle.partitions")
      .map(_.getTuneValue().toInt)

    // scalastyle:off line.size.limit
    // Determine whether to recommend initialPartitionNum based on shuffle partitions recommendation
    recShufflePartitions match {
      case Some(shufflePartitions) if shufflePartitions >= recInitialPartitionNum =>
        // Skip recommending 'initialPartitionNum' when:
        // - AutoTuner has already recommended 'spark.sql.shuffle.partitions' AND
        // - The recommended shuffle partitions value is sufficient (>= recInitialPartitionNum)
        // This is because AQE will use the recommended 'spark.sql.shuffle.partitions' by default.
        // Reference: https://spark.apache.org/docs/latest/sql-performance-tuning.html#coalescing-post-shuffle-partitions
      case _ =>
        // Set 'initialPartitionNum' when either:
        // - AutoTuner has not recommended 'spark.sql.shuffle.partitions' OR
        // - Recommended shuffle partitions is small (< recInitialPartitionNum)
        appendRecommendation(getInitialPartitionNumProperty, recInitialPartitionNum)
        appendRecommendation("spark.sql.shuffle.partitions", recInitialPartitionNum)
    }
    // scalastyle:on line.size.limit

    // TODO - can we set spark.sql.autoBroadcastJoinThreshold ???
    val autoBroadcastJoinKey = "spark.sql.adaptive.autoBroadcastJoinThreshold"
    val autoBroadcastJoinThresholdPropertyMB =
      getPropertyValue(autoBroadcastJoinKey).map(StringUtils.convertToMB(_, Some(ByteUnit.BYTE)))
    val autoBroadcastJoinThresholdDefaultMB =
      tuningConfigs.getEntry("AQE_AUTO_BROADCAST_JOIN_THRESHOLD").getDefaultAsMemory(ByteUnit.MiB)
    if (autoBroadcastJoinThresholdPropertyMB.isEmpty) {
      appendComment(autoBroadcastJoinKey, s"'$autoBroadcastJoinKey' was not set.")
    } else if (autoBroadcastJoinThresholdPropertyMB.get > autoBroadcastJoinThresholdDefaultMB) {
      appendComment(s"Setting '$autoBroadcastJoinKey' > ${autoBroadcastJoinThresholdDefaultMB}m" +
        s" could lead to performance\n" +
        "  regression. Should be set to a lower number.")
    }
  }

  /**
   * Checks the system properties and give feedback to the user.
   * For example file.encoding=UTF-8 is required for some ops like GpuRegEX.
   */
  private def recommendSystemProperties(): Unit = {
    appInfoProvider.getSystemProperty("file.encoding").collect {
      case encoding if !ToolUtils.isFileEncodingRecommended(encoding) =>
        appendComment(s"file.encoding should be [${ToolUtils.SUPPORTED_ENCODINGS.mkString}]" +
            " because GPU only supports the charset when using some expressions.")
    }
  }

  /**
   * Check the class path entries with the following rules:
   * 1- If ".*rapids-4-spark.*jar" is missing then add a comment that the latest jar should be
   *    included in the classpath unless it is part of the spark
   * 2- If there are more than 1 entry for ".*rapids-4-spark.*jar", then add a comment that
   *    there should be only 1 jar in the class path.
   * 3- If there are cudf jars, ignore that for now.
   * 4- If there is a new release recommend that to the user
   */
  private def recommendClassPathEntries(): Unit = {
    val missingRapidsJarsEntry = classPathComments("rapids.jars.missing")
    val multipleRapidsJarsEntry = classPathComments("rapids.jars.multiple")

    appInfoProvider.getRapidsJars match {
      case Seq() =>
        // No rapids jars
        appendComment(missingRapidsJarsEntry)
      case s: Seq[String] =>
        s.flatMap(e =>
          autoTunerHelper.pluginJarRegEx.findAllMatchIn(e).map(_.group(1))) match {
            case Seq() => appendComment(missingRapidsJarsEntry)
            case v: Seq[String] if v.length > 1 =>
              val comment = s"$multipleRapidsJarsEntry [${v.mkString(", ")}]"
              appendComment(comment)
            case Seq(jarVer) =>
              // compare jarVersion to the latest release
              val latestPluginVersion = WebCrawlerUtil.getLatestPluginRelease
              latestPluginVersion match {
                case Some(ver) =>
                  if (ToolUtils.compareVersions(jarVer, ver) < 0) {
                    val jarURL = WebCrawlerUtil.getPluginMvnDownloadLink(ver)
                    appendComment(
                      "A newer RAPIDS Accelerator for Apache Spark plugin is available:\n" +
                        s"  $jarURL\n" +
                        s"  Version used in application is $jarVer.")
                  }
                case None =>
                  logError("Could not pull the latest release of RAPIDS-plugin jar.")
                  val pluginRepoUrl = WebCrawlerUtil.getMVNArtifactURL("rapids.plugin")
                  appendComment(
                    "Failed to validate the latest release of Apache Spark plugin.\n" +
                    s"  Verify that the version used in application ($jarVer) is the latest on:\n" +
                    s"  $pluginRepoUrl")

            }
        }
    }
  }

  /**
   * Calculate max partition bytes using the max task input size and existing setting
   * for maxPartitionBytes. Note that this won't apply the same on iceberg.
   * The max bytes here does not distinguish between GPU and CPU reads so we could
   * improve that in the future.
   * Eg,
   * TASK_INPUT_SIZE_THRESHOLD = {min: 128m, max: 256m}
   * (1) Input:  currentMaxPartitionBytes = 512m
   *             actualTaskInputSizeMB = 12m (below min threshold -> increase maxPartitionBytes)
   *     Output: recommendedMaxPartitionBytes = 512m * (128m/12m) = 4g (hit max value)
   * (2) Input:  currentMaxPartitionBytes = 2g
   *             actualTaskInputSizeMB = 512m (above max threshold -> decrease maxPartitionBytes)
   *     Output: recommendedMaxPartitionBytes = 2g / (512m/128m) = 512m
   */
  protected def calculateMaxPartitionBytesInMB(currentMaxPartitionBytes: String): Option[Long] = {
    // AutoTuner only supports a single app right now, so we get whatever value is here
    val actualTaskInputSizeMB = appInfoProvider.getMaxInput / 1024 / 1024
    val currentMaxPartitionBytesMB = StringUtils.convertToMB(
      currentMaxPartitionBytes, Some(ByteUnit.BYTE))
    // Get the min and max thresholds for the task input size
    val taskInputSizeThreshold = tuningConfigs.getEntry("TASK_INPUT_SIZE_THRESHOLD")
    val minTaskInputSizeThresholdMB = taskInputSizeThreshold.getMinAsMemory(ByteUnit.MiB)
    val maxTaskInputSizeThresholdMB = taskInputSizeThreshold.getMaxAsMemory(ByteUnit.MiB)
    // Get the upper bound for the max partition bytes
    val maxAllowedPartitionBytesMB =
      tuningConfigs.getEntry("MAX_PARTITION_BYTES").getMaxAsMemory(ByteUnit.MiB)

    if (actualTaskInputSizeMB == 0.0) {
      Some(currentMaxPartitionBytesMB)
    } else {
      if (actualTaskInputSizeMB > 0 && actualTaskInputSizeMB < minTaskInputSizeThresholdMB) {
        // If task input too small (< min threshold): increase partition size to get bigger tasks
        val recommendedMaxPartitionBytesMB = Math.min(
          currentMaxPartitionBytesMB * (minTaskInputSizeThresholdMB / actualTaskInputSizeMB),
          maxAllowedPartitionBytesMB)
        Some(recommendedMaxPartitionBytesMB.toLong)
      } else if (actualTaskInputSizeMB > maxTaskInputSizeThresholdMB) {
        //  If task input too large (> max threshold): decrease partition size to get smaller tasks
        val recommendedMaxPartitionBytesMB = Math.min(
          currentMaxPartitionBytesMB / (actualTaskInputSizeMB / maxTaskInputSizeThresholdMB),
          maxAllowedPartitionBytesMB)
        Some(recommendedMaxPartitionBytesMB.toLong)
      } else {
        // If task input within range: no adjustment needed
        None
      }
    }
  }

  /**
   * Recommendation for 'spark.rapids.file.cache' based on read characteristics of job.
   */
  private def recommendFileCache(): Unit = {
    if (appInfoProvider.getDistinctLocationPct <
        tuningConfigs.getEntry("DISTINCT_READ_THRESHOLD").getDefault.toDouble &&
      appInfoProvider.getRedundantReadSize >
        tuningConfigs.getEntry("READ_SIZE_THRESHOLD").getDefaultAsMemory(ByteUnit.BYTE)) {
      appendRecommendation("spark.rapids.filecache.enabled",
        tuningConfigs.getEntry("FILE_CACHE_ENABLED").getDefault)
      appendComment("Enable file cache only if Spark local disks bandwidth is > 1 GB/s" +
        " and you have sufficient disk space available to fit both cache and normal Spark" +
        " temporary data.")
    }
  }

  /**
   * Recommendation for 'spark.sql.files.maxPartitionBytes' based on input size for each task.
   * Note that the logic can be disabled by adding the property to "limitedLogicRecommendations"
   * which is one of the arguments of [[getRecommendedProperties]].
   */
  private def recommendMaxPartitionBytes(): Unit = {
    val maxPartitionProp =
      getPropertyValue("spark.sql.files.maxPartitionBytes")
        .getOrElse(tuningConfigs.getEntry("MAX_PARTITION_BYTES").getDefault)
    val recommended =
      if (isCalculationEnabled("spark.sql.files.maxPartitionBytes")) {
        calculateMaxPartitionBytesInMB(maxPartitionProp).map(_.toString).orNull
      } else {
        s"${StringUtils.convertToMB(maxPartitionProp, Some(ByteUnit.BYTE))}"
      }
    appendRecommendationForMemoryMB("spark.sql.files.maxPartitionBytes", recommended)
  }

  /**
   * Internal method to recommend 'spark.sql.shuffle.partitions' based on spills and skew.
   * This method can be overridden by Profiling/Qualification AutoTuners to provide custom logic.
   */
  protected def recommendShufflePartitionsInternal(inputShufflePartitions: Int): Int = {
    var shufflePartitions = inputShufflePartitions
    val lookup = "spark.sql.shuffle.partitions"
    val shuffleStagesWithPosSpilling = appInfoProvider.getShuffleStagesWithPosSpilling
    if (shuffleStagesWithPosSpilling.nonEmpty) {
      val shuffleSkewStages = appInfoProvider.getShuffleSkewStages
      if (shuffleSkewStages.exists(id => shuffleStagesWithPosSpilling.contains(id))) {
        appendOptionalComment(lookup,
          "Shuffle skew exists (when task's Shuffle Read Size > 3 * Avg Stage-level size) in\n" +
            s"  stages with spilling. Increasing shuffle partitions is not recommended in this\n" +
            s"  case since keys will still hash to the same task.")
      } else {
        shufflePartitions *=
          tuningConfigs.getEntry("SHUFFLE_PARTITION_MULTIPLIER").getDefault.toInt
        // Could be memory instead of partitions
        appendOptionalComment(lookup,
          s"'$lookup' should be increased since spilling occurred in shuffle stages.")
      }
    }
    shufflePartitions
  }

  /**
   * Recommendations for 'spark.sql.shuffle.partitions' based on spills and skew in shuffle stages.
   * Note that the logic can be disabled by adding the property to "limitedLogicRecommendations"
   * which is one of the arguments of [[getRecommendedProperties]].
   */
  private def recommendShufflePartitions(): Unit = {
    val lookup = "spark.sql.shuffle.partitions"
    var shufflePartitions = getPropertyValue(lookup).getOrElse(
      tuningConfigs.getEntry("SHUFFLE_PARTITIONS").getDefault).toInt

    // TODO: Need to look at other metrics for GPU spills (DEBUG mode), and batch sizes metric
    if (isCalculationEnabled(lookup)) {
      shufflePartitions = recommendShufflePartitionsInternal(shufflePartitions)
    }
    val aqeAutoShuffle = getPropertyValue("spark.databricks.adaptive.autoOptimizeShuffle.enabled")
    if (aqeAutoShuffle.isDefined) {
      // If the user has enabled AQE auto shuffle, override with the default
      // recommendation for that property.
      appendRecommendation("spark.databricks.adaptive.autoOptimizeShuffle.enabled",
        tuningConfigs.getEntry("DATABRICKS_AUTO_OPTIMIZE_SHUFFLE_ENABLED").getDefault)
    }
    appendRecommendation("spark.sql.shuffle.partitions", s"$shufflePartitions")
  }

  /**
   * Analyzes unsupported driver logs and generates recommendations for configuration properties.
   */
  private def recommendFromDriverLogs(): Unit = {
    // Iterate through unsupported operators' reasons and check for matching properties
    driverInfoProvider.getUnsupportedOperators.map(_.reason).foreach { operatorReason =>
      autoTunerHelper.unsupportedOperatorRecommendations.collect {
        case (config, recommendedValue) if operatorReason.contains(config) =>
          appendRecommendation(config, recommendedValue)
          appendComment(commentForExperimentalConfig(config))
      }
    }
  }

  private def recommendPluginProps(): Unit = {
    val isPluginLoaded = getPropertyValue("spark.plugins") match {
      case Some(f) => f.contains("com.nvidia.spark.SQLPlugin")
      case None => false
    }
    // Set the plugin to True without need to check if it is already set.
    appendRecommendation("spark.rapids.sql.enabled", "true")
    if (!isPluginLoaded) {
      appendComment("RAPIDS Accelerator for Apache Spark jar is missing in \"spark.plugins\". " +
        "Please refer to " +
        "https://docs.nvidia.com/spark-rapids/user-guide/latest/getting-started/overview.html")
    }
  }

  def appendOptionalComment(lookup: String, comment: String): Unit = {
    if (!skippedRecommendations.contains(lookup)) {
      appendComment(comment)
    }
  }

  def appendComment(comment: String): Unit = {
    comments += comment
  }

  /**
   * Adds a comment for a configuration key when AutoTuner cannot provide a recommended value,
   * but the configuration is necessary.
   */
  private def appendComment(
      key: String,
      comment: String,
      fillInValue: Option[String] = None): Unit = {
    if (!skippedRecommendations.contains(key)) {
      val recomRecord = recommendations.getOrElseUpdate(key,
        TuningEntry.build(key, getPropertyValueFromSource(key), None, finalTuningTable.get(key)))
      recomRecord.markAsUnresolved(fillInValue)
      comments += comment
    }
  }

  def convertClusterPropsToString(): String = {
    clusterProps.toString
  }

  /**
   * Add default comments for missing properties except the ones
   * which should be skipped.
   */
  private def addDefaultComments(): Unit = {
    appendComment("Could not infer the cluster configuration, recommendations " +
      "are generated using default values!")
    commentsForMissingProps.foreach {
      case (key, value) =>
        if (!skippedRecommendations.contains(key)) {
          appendComment(value)
        }
    }
  }

  private def addMissingMemoryComments(): Unit = {
    commentsForMissingMemoryProps.foreach {
      case (key, value) =>
        if (!skippedRecommendations.contains(key)) {
          appendComment(value)
        }
    }
  }

  private def toCommentProfileResult: Seq[RecommendedCommentResult] = {
    comments.map(RecommendedCommentResult).sortBy(_.comment)
  }

  private def toRecommendationsProfileResult: Seq[TuningEntryTrait] = {
    val recommendationEntries = if (filterByUpdatedPropertiesEnabled) {
      recommendations.values.filter(_.isTuned())
    } else {
      recommendations.values.filter(_.isEnabled())
    }
    recommendationEntries.toSeq.sortBy(_.name)
  }

  protected def finalizeTuning(): Unit = {
    recommendations.values.foreach(_.commit())
  }

  /**
   * The Autotuner loads the spark properties from either the ClusterProperties or the eventlog.
   * 1- runs the calculation for each criterion and saves it as a [[TuningEntryTrait]].
   * 2- The final list of recommendations include any [[TuningEntryTrait]] that has a
   *    recommendation that is different from the original property.
   * 3- Null values are excluded.
   * 4- A comment is added for each missing property in the spark property.
   *
   * @param skipList a list of properties to be skipped. If none, all recommendations are
   *                 returned. Note that the recommendations will be computed anyway internally
   *                 in case there are dependencies between the recommendations.
   *                 Default is empty.
   * @param limitedLogicList a list of properties that will do simple recommendations based on
   *                         static default values.
   * @param showOnlyUpdatedProps When enabled, the profiler recommendations should only include
   *                             updated settings.
   * @return pair of recommendations and comments. Both sequence can be empty.
   */
  def getRecommendedProperties(
      skipList: Option[Seq[String]] = Some(Seq()),
      limitedLogicList: Option[Seq[String]] = Some(Seq()),
      showOnlyUpdatedProps: Boolean = true):
      (Seq[TuningEntryTrait], Seq[RecommendedCommentResult]) = {
    if (appInfoProvider.isAppInfoAvailable) {
      limitedLogicList.foreach(limitedSeq => limitedLogicRecommendations ++= limitedSeq)
      skipList.foreach(skipSeq => skippedRecommendations ++= skipSeq)
      skippedRecommendations ++= platform.recommendationsToExclude
      initRecommendations()
      // update GPU device of platform based on cluster properties if it is not already set.
      // if the GPU device cannot be inferred from cluster properties, do not make any updates.
      if (platform.gpuDevice.isEmpty && !clusterProps.isEmpty && !clusterProps.gpu.isEmpty) {
        GpuDevice.createInstance(clusterProps.gpu.getName)
          .foreach(platform.setGpuDevice)
      }
      // configured GPU recommended instance type NEEDS to happen before any of the other
      // recommendations as they are based on
      // the instance type
      configureGPURecommendedInstanceType()
      configureClusterPropDefaults
      // Makes recommendations based on information extracted from the AppInfoProvider
      filterByUpdatedPropertiesEnabled = showOnlyUpdatedProps
      recommendPluginProps
      calculateJobLevelRecommendations()
      calculateClusterLevelRecommendations()

      // Add all platform specific recommendations
      platform.platformSpecificRecommendations.collect {
        case (property, value) if getPropertyValueFromSource(property).isEmpty =>
          appendRecommendation(property, value)
      }
    }
    recommendFromDriverLogs()
    finalizeTuning()
    (toRecommendationsProfileResult, toCommentProfileResult)
  }

  // Process the properties keys. This is needed in case there are some properties that should not
  // be listed in the final combined results. For example:
  // - The UUID of the app is not part of the submitted spark configurations
  // - make sure that we exclude the skipped list
  private def processPropKeys(
      srcMap: collection.Map[String, String]): collection.Map[String, String] = {
    srcMap -- skippedRecommendations
  }

  // Combines the original Spark properties with the recommended ones.
  def combineSparkProperties(
      recommendedSet: Seq[TuningEntryTrait]): Seq[RecommendedPropertyResult] = {
    // get the original properties after filtering and removing unnecessary keys
    val originalPropsFiltered = processPropKeys(getAllSourceProperties)
    // Combine the original properties with the recommended properties.
    // The recommendations should always override the original ones
    val combinedProps = (originalPropsFiltered
      ++ recommendedSet.map(r => r.name -> r.getTuneValue()).toMap).toSeq.sortBy(_._1)
    combinedProps.collect {
      case (pK, pV) => RecommendedPropertyResult(pK, pV)
    }
  }

  /**
   * Gets the initial partition number property key.
   * @return the property key to use for initial partition number
   */
  private def getInitialPartitionNumProperty: String = {
    val maxNumPostShufflePartitions = "spark.sql.adaptive.maxNumPostShufflePartitions"
    val initialPartitionNumKey = "spark.sql.adaptive.coalescePartitions.initialPartitionNum"
    // check if maxNumPostShufflePartitions is in final tuning table
    if (finalTuningTable.contains(maxNumPostShufflePartitions)) {
      maxNumPostShufflePartitions
    } else {
      initialPartitionNumKey
    }
  }

  /**
   * Gets the initial partition number value using the appropriate property key.
   * @return the initial partition number value if found
   */
  private def getInitialPartitionNumValue: Option[String] = {
    val propertyKey = getInitialPartitionNumProperty
    getPropertyValue(propertyKey)
  }

}

object AutoTuner {
  /**
   * Helper function to get a combined property function that can be used
   * to retrieve the value of a property in the following priority order:
   * 1. From the recommendations map
   *    - This will include the user-enforced Spark properties
   *    - This implies the properties to be present in the target application
   * 2. From the source Spark properties
   */
  def getCombinedPropertyFn(
    recommendations: mutable.LinkedHashMap[String, TuningEntryTrait],
    sourceSparkProperties: Map[String, String]): String => Option[String] = {
    (key: String) => {
      recommendations.get(key).map(_.getTuneValue())
        .orElse(sourceSparkProperties.get(key))
    }
  }
}

/**
 * Implementation of the `AutoTuner` specific for the Profiling Tool.
 * This class implements the logic to recommend AutoTuner configurations
 * specifically for GPU event logs.
 */
class ProfilingAutoTuner(
    clusterProps: ClusterProperties,
    appInfoProvider: BaseProfilingAppSummaryInfoProvider,
    platform: Platform,
    driverInfoProvider: DriverLogInfoProvider,
    userProvidedTuningConfigs: Option[TuningConfigsProvider])
  extends AutoTuner(clusterProps, appInfoProvider, platform, driverInfoProvider,
    userProvidedTuningConfigs, ProfilingAutoTunerHelper) {

  /**
   * Overrides the calculation for 'spark.sql.files.maxPartitionBytes'.
   * Logic:
   * - First, calculate the recommendation based on input sizes (parent implementation).
   * - If GPU OOM errors occurred in scan stages,
   *     - If calculated value is defined, choose the minimum between the calculated value and
   *       half of the current value.
   *     - Else, halve the current value.
   * - Else, use the value from the parent implementation.
   */
  override def calculateMaxPartitionBytesInMB(maxPartitionBytes: String): Option[Long] = {
    // First, calculate the recommendation based on input sizes
    val calculatedValueFromInputSize = super.calculateMaxPartitionBytesInMB(maxPartitionBytes)
    getPropertyValue("spark.sql.files.maxPartitionBytes") match {
      case Some(currentValue) if appInfoProvider.hasScanStagesWithGpuOom =>
        // GPU OOM detected. We may want to reduce max partition size.
        val halvedValue = StringUtils.convertToMB(currentValue, Some(ByteUnit.BYTE)) / 2
        // Choose the minimum between the calculated value and half of the current value.
        calculatedValueFromInputSize match {
          case Some(calculatedValue) => Some(math.min(calculatedValue, halvedValue))
          case None => Some(halvedValue)
        }
      case _ =>
        // Else, use the value from the parent implementation
        calculatedValueFromInputSize
    }
  }

  /**
   * Overrides the calculation for 'spark.sql.shuffle.partitions'.
   * This method checks for task OOM errors in shuffle stages and recommends to increase
   * shuffle partitions if task OOM errors occurred.
   */
  override def recommendShufflePartitionsInternal(inputShufflePartitions: Int): Int = {
    val calculatedValue = super.recommendShufflePartitionsInternal(inputShufflePartitions)
    val lookup = "spark.sql.shuffle.partitions"
    val currentValue = getPropertyValue(lookup).getOrElse(
      tuningConfigs.getEntry("SHUFFLE_PARTITIONS").getDefault).toInt
    if (appInfoProvider.hasShuffleStagesWithOom) {
      // Shuffle Stages with Task OOM detected. We may want to increase shuffle partitions.
      val recShufflePartitions = currentValue *
        tuningConfigs.getEntry("SHUFFLE_PARTITION_MULTIPLIER").getDefault.toInt
      appendOptionalComment(lookup,
        s"'$lookup' should be increased since task OOM occurred in shuffle stages.")
      math.max(calculatedValue, recShufflePartitions)
    } else {
      // Else, return the calculated value from the parent implementation
      calculatedValue
    }
  }
}

/**
 * Helper trait for the AutoTuner
 */
trait AutoTunerHelper extends Logging {
  /**
   * Default strategy for cluster shape recommendation.
   * See [[com.nvidia.spark.rapids.tool.ClusterSizingStrategy]] for different strategies.
   */
  lazy val recommendedClusterSizingStrategy: ClusterSizingStrategy = ConstantGpuCountStrategy
  // the plugin jar is in the form of rapids-4-spark_scala_binary-(version)-*.jar
  lazy val pluginJarRegEx: Regex = "rapids-4-spark_\\d\\.\\d+-(\\d{2}\\.\\d{2}\\.\\d+).*\\.jar".r
  lazy val gpuKryoRegistratorClassName = "com.nvidia.spark.rapids.GpuKryoRegistrator"

  // Recommended values for specific unsupported configurations
  lazy val unsupportedOperatorRecommendations: Map[String, String] = Map(
    "spark.rapids.sql.incompatibleDateFormats.enabled" -> "true"
  )

  /**
   * Abstract method to create an instance of the AutoTuner.
   */
  def createAutoTunerInstance(
    clusterProps: ClusterProperties,
    appInfoProvider: AppSummaryInfoBaseProvider,
    platform: Platform,
    driverInfoProvider: DriverLogInfoProvider,
    userProvidedTuningConfigs: Option[TuningConfigsProvider]): AutoTuner

  def handleException(
      ex: Throwable,
      appInfo: AppSummaryInfoBaseProvider,
      platform: Platform,
      driverInfoProvider: DriverLogInfoProvider,
      userProvidedTuningConfigs: Option[TuningConfigsProvider]): AutoTuner = {
    logError("Exception: " + ex.getStackTrace.mkString("Array(", ", ", ")"))
    val tuning = createAutoTunerInstance(new ClusterProperties(), appInfo,
      platform, driverInfoProvider, userProvidedTuningConfigs)
    val msg = ex match {
      case cEx: ConstructorException => cEx.getContext
      case _ => if (ex.getCause != null) ex.getCause.toString else ex.toString
    }
    tuning.appendComment(msg)
    tuning
  }

  /**
   * Similar to [[buildAutoTuner]] but it allows constructing the AutoTuner without an
   * existing file. This can be used in testing.
   *
   * @param clusterProps the cluster properties as string.
   * @param singleAppProvider the wrapper implementation that accesses the properties of the profile
   *                          results.
   * @param platform represents the environment created as a target for recommendations.
   * @param driverInfoProvider wrapper implementation that accesses the information from driver log.
   * @return a new AutoTuner object.
   */
  def buildAutoTunerFromProps(
      clusterProps: String,
      singleAppProvider: AppSummaryInfoBaseProvider,
      platform: Platform = PlatformFactory.createInstance(clusterProperties = None),
      driverInfoProvider: DriverLogInfoProvider = BaseDriverLogInfoProvider.noneDriverLog,
      userProvidedTuningConfigs: Option[TuningConfigsProvider] = None
  ): AutoTuner = {
    try {
      val clusterPropsOpt = PropertiesLoader[ClusterProperties].loadFromContent(clusterProps)
      createAutoTunerInstance(clusterPropsOpt.getOrElse(new ClusterProperties()),
        singleAppProvider, platform, driverInfoProvider, userProvidedTuningConfigs)
    } catch {
      case NonFatal(e) =>
        handleException(e, singleAppProvider, platform, driverInfoProvider,
          userProvidedTuningConfigs)
    }
  }

  def buildAutoTuner(
      singleAppProvider: AppSummaryInfoBaseProvider,
      platform: Platform,
      driverInfoProvider: DriverLogInfoProvider = BaseDriverLogInfoProvider.noneDriverLog,
      userProvidedTuningConfigs: Option[TuningConfigsProvider] = None
  ): AutoTuner = {
    try {
      val autoT = createAutoTunerInstance(
        platform.clusterProperties.getOrElse(new ClusterProperties()),
        singleAppProvider, platform, driverInfoProvider, userProvidedTuningConfigs)
      autoT
    } catch {
      case NonFatal(e) =>
        handleException(e, singleAppProvider, platform, driverInfoProvider,
          userProvidedTuningConfigs)
    }
  }

  def buildShuffleManagerClassName(smVersion: String): String = {
    s"com.nvidia.spark.rapids.spark$smVersion.RapidsShuffleManager"
  }
}

/**
 * Provides configuration settings for the Profiling Tool's AutoTuner. This object is as a concrete
 * implementation of the `AutoTunerHelper` interface.
 */
object ProfilingAutoTunerHelper extends AutoTunerHelper {
  def createAutoTunerInstance(
      clusterProps: ClusterProperties,
      appInfoProvider: AppSummaryInfoBaseProvider,
      platform: Platform,
      driverInfoProvider: DriverLogInfoProvider,
      userProvidedTuningConfigs: Option[TuningConfigsProvider]): AutoTuner = {
    appInfoProvider match {
      case profilingAppProvider: BaseProfilingAppSummaryInfoProvider =>
        new ProfilingAutoTuner(clusterProps, profilingAppProvider, platform,
          driverInfoProvider, userProvidedTuningConfigs)
      case _ =>
        throw new IllegalArgumentException("'appInfoProvider' must be an instance of " +
          s"${classOf[BaseProfilingAppSummaryInfoProvider]}")
    }
  }
}

/**
 * Trait providing static comments for the AutoTuner.
 * The static comments are used in unit tests as well.
 */
trait AutoTunerStaticComments {
  // scalastyle:off line.size.limit
  private lazy val advancedConfigDocUrl = "https://nvidia.github.io/spark-rapids/docs/additional-functionality/advanced_configs.html#advanced-configuration"
  private lazy val shuffleManagerDocUrl = "https://docs.nvidia.com/spark-rapids/user-guide/latest/additional-functionality/rapids-shuffle.html#rapids-shuffle-manager"

  val classPathComments: Map[String, String] = Map(
    "rapids.jars.missing" ->
      ("RAPIDS Accelerator for Apache Spark plugin jar is missing\n" +
        "  from the classpath entries.\n" +
        "  If the Spark RAPIDS jar is being bundled with your\n" +
        "  Spark distribution, this step is not needed."),
    "rapids.jars.multiple" ->
      ("Multiple RAPIDS Accelerator for Apache Spark plugin jar\n" +
        "  exist on the classpath.\n" +
        "  Make sure to keep only a single jar."),
    "rapids.shuffle.jars" ->
      ("The RAPIDS Shuffle Manager requires spark.driver.extraClassPath\n" +
        "  and spark.executor.extraClassPath settings to include the\n" +
        "  path to the Spark RAPIDS plugin jar.\n" +
        "  If the Spark RAPIDS jar is being bundled with your Spark\n" +
        "  distribution, this step is not needed.")
  )

  def shuffleManagerCommentForUnsupportedVersion(sparkVersion: String, platform: Platform): String = {
    val (latestSparkVersion, latestSmVersion) = platform.latestSupportedShuffleManagerInfo
    s"""
       |Cannot recommend RAPIDS Shuffle Manager for unsupported ${platform.sparkVersionLabel}: '$sparkVersion'.
       |To enable RAPIDS Shuffle Manager, use a supported ${platform.sparkVersionLabel} (e.g., '$latestSparkVersion')
       |and set: '--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark$latestSmVersion.RapidsShuffleManager'.
       |See supported versions: $shuffleManagerDocUrl.
       |""".stripMargin.trim.replaceAll("\n", "\n  ")
  }
  // scalastyle:on line.size.limit

  def shuffleManagerCommentForMissingVersion: String = {
    "Could not recommend RapidsShuffleManager as Spark version cannot be determined."
  }

  def latestPluginJarComment(latestJarMvnUrl: String, currentJarVer: String): String = {
    s"""
       |A newer RAPIDS Accelerator for Apache Spark plugin is available:
       |$latestJarMvnUrl
       |Version used in application is $currentJarVer.
       |""".stripMargin.trim.replaceAll("\n", "\n  ")
  }

  def notEnoughMemComment(minSizeInMB: Long): String = {
    s"""
       |This node/worker configuration is not ideal for using the RAPIDS Accelerator
       |for Apache Spark because it doesn't have enough memory for the executors.
       |We recommend either using nodes with more memory or reducing 'spark.memory.offHeap.size',
       |as off-heap memory is unused by the RAPIDS Accelerator, unless explicitly required by
       |the application. Need at least $minSizeInMB MB memory per executor.
       |""".stripMargin.trim.replaceAll("\n", "\n  ")
  }

  def notEnoughMemCommentForKey(key: String): String = {
    s"Not enough memory to set '$key'. See comments for more details."
  }

  /**
   * Append a comment to the list indicating that the property was enforced by the user.
   * @param key the property set by the autotuner.
   */
  def getEnforcedPropertyComment(key: String): String = {
    s"'$key' was user-enforced in the target cluster properties."
  }

  def commentForExperimentalConfig(config: String): String = {
    s"Using $config does not guarantee to produce the same results as CPU. " +
      s"Please refer to $advancedConfigDocUrl."
  }
}

/**
 * Trait providing default comments for missing or recommended Spark properties,
 * using values from the provided tuning configuration.
 * Class mixing in this trait must provide a `tuningConfigs` instance.
 */
trait AutoTunerCommentsWithTuningConfigs {
  val tuningConfigs: TuningConfigsProvider

  /**
   * Helper function to generate a comment for a missing property.
   */
  private def generateMissingComment(property: String, recommendation: String): String = {
    s"'$property' should be set to $recommendation."
  }

  // scalastyle:off line.size.limit
  protected val commentsForMissingMemoryProps: Map[String, String] = Map(
    "spark.executor.memory" ->
      generateMissingComment("spark.executor.memory",
        s"${tuningConfigs.getEntry("HEAP_PER_CORE").getDefault}/core"),
    "spark.rapids.memory.pinnedPool.size" ->
      generateMissingComment("spark.rapids.memory.pinnedPool.size",
        tuningConfigs.getEntry("PINNED_MEMORY").getDefault))

  protected val commentsForMissingProps: Map[String, String] = Map(
    "spark.executor.cores" ->
      // TODO: This could be extended later to be platform specific.
      generateMissingComment("spark.executor.cores",
        tuningConfigs.getEntry("CORES_PER_EXECUTOR").getDefault),
    "spark.executor.instances" ->
      generateMissingComment("spark.executor.instances",
        "(cpuCoresPerNode * numWorkers) / 'spark.executor.cores'"),
    "spark.task.resource.gpu.amount" ->
      generateMissingComment("spark.task.resource.gpu.amount",
        tuningConfigs.getEntry("TASK_GPU_RESOURCE_AMT").getDefault),
    "spark.rapids.sql.concurrentGpuTasks" ->
      generateMissingComment("spark.rapids.sql.concurrentGpuTasks",
        s"Min(${tuningConfigs.getEntry("CONC_GPU_TASKS").getMax.toLong}, " +
          s"(gpuMemory / ${tuningConfigs.getEntry("GPU_MEM_PER_TASK").getDefault}))"),
    "spark.rapids.sql.enabled" ->
      "'spark.rapids.sql.enabled' should be true to enable SQL operations on the GPU.",
    "spark.sql.adaptive.enabled" ->
      "'spark.sql.adaptive.enabled' should be enabled for better performance."
  ) ++ commentsForMissingMemoryProps
  // scalastyle:off line.size.limit
}
