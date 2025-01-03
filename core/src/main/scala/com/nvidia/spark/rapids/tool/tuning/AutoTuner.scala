/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION.
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

import java.io.{BufferedReader, InputStreamReader, IOException}
import java.util

import scala.beans.BeanProperty
import scala.collection.{mutable, Seq}
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal
import scala.util.matching.Regex

import com.nvidia.spark.rapids.tool.{AppSummaryInfoBaseProvider, GpuDevice, Platform, PlatformFactory}
import com.nvidia.spark.rapids.tool.profiling._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, Path}
import org.yaml.snakeyaml.{DumperOptions, LoaderOptions, Yaml}
import org.yaml.snakeyaml.constructor.{Constructor, ConstructorException}
import org.yaml.snakeyaml.representer.Representer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.ToolUtils
import org.apache.spark.sql.rapids.tool.util.{StringUtils, WebCrawlerUtil}

/**
 * A wrapper class that stores all the GPU properties.
 * The BeanProperty enables loading and parsing the YAML formatted content using the
 * Constructor SnakeYaml approach.
 */
class GpuWorkerProps(
    @BeanProperty var memory: String,
    @BeanProperty var count: Int,
    @BeanProperty var name: String) {
  def this() {
    this("0m", 0, "None")
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
  def setDefaultGpuCountIfMissing(autoTunerConfigsProvider: AutoTunerConfigsProvider): Boolean = {
    // TODO - do we want to recommend 1 or base it on core count?  32 cores to 1 gpu may be to much.
    if (count == 0) {
      count = autoTunerConfigsProvider.DEF_WORKER_GPU_COUNT
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
      autoTunerConfigsProvider: AutoTunerConfigsProvider): Seq[String] = {
    val res = new ListBuffer[String]()
    if (setDefaultGpuCountIfMissing(autoTunerConfigsProvider)) {
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
  def this() {
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
  def setDefaultNumWorkersIfMissing(autoTunerConfigsProvider: AutoTunerConfigsProvider): Boolean = {
    if (numWorkers <= 0) {
      numWorkers = autoTunerConfigsProvider.DEF_NUM_WORKERS
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
  def setMissingFields(autoTunerConfigsProvider: AutoTunerConfigsProvider): Seq[String] = {
    val res = new ListBuffer[String]()
    if (setDefaultNumWorkersIfMissing(autoTunerConfigsProvider)) {
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
    @BeanProperty var softwareProperties: util.LinkedHashMap[String, String]) {

  def this() {
    this(new SystemClusterProps(), new GpuWorkerProps(), new util.LinkedHashMap[String, String]())
  }
  def isEmpty: Boolean = {
    system.isEmpty && gpu.isEmpty
  }
  override def toString: String =
    s"{${system.toString}, ${gpu.toString}, $softwareProperties}"
}

/**
 * Wrapper to hold the recommendation of a given criterion.
 *
 * @param name the property label.
 * @param original the value loaded from the spark properties.
 * @param recommended the recommended value by the AutoTuner.
 */
class RecommendationEntry(val name: String,
    val original: Option[String],
    var recommended: Option[String]) {

  def setRecommendedValue(value: String): Unit = {
    recommended = Option(value)
  }

  /**
   * Used to compare between two properties by converting memory units to
   * a equivalent representations.
   * @param propValue property to be processed.
   * @return the uniform representation of property.
   *         For Memory, the value is converted to bytes.
   */
  private def getRawValue(propValue: Option[String]): Option[String] = {
    propValue match {
      case None => None
      case Some(value) =>
        if (StringUtils.isMemorySize(value)) {
          // if it is memory return the bytes unit
          Some(s"${StringUtils.convertMemorySizeToBytes(value)}")
        } else {
          propValue
        }
    }
  }

  /**
   * Returns true when the recommendation is different than the original.
   */
  private def recommendsNewValue(): Boolean = {
    val originalVal = getRawValue(original)
    val recommendedVal = getRawValue(recommended)
    (originalVal, recommendedVal) match {
      case (None, None) => false
      case (Some(orig), Some(rec)) =>
        orig != rec
      case _ => true
    }
  }

  /**
   * True or False whether the recommendation is valid. e.g., recommendations that does not change
   * the original value returns false if filter is enabled.
   * @param filterByUpdated flag to pick only the properties that would be updated by the
   *                        recommendations
   */
  def isValid(filterByUpdated: Boolean): Boolean = {
    recommended match {
      case None => false
      case _ =>
        if (filterByUpdated) { // filter enabled
          recommendsNewValue()
        } else {
          true
        }
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
class AutoTuner(
    val clusterProps: ClusterProperties,
    val appInfoProvider: AppSummaryInfoBaseProvider,
    val platform: Platform,
    val driverInfoProvider: DriverLogInfoProvider,
    val autoTunerConfigsProvider: AutoTunerConfigsProvider)
  extends Logging {

  var comments = new ListBuffer[String]()
  var recommendations: mutable.LinkedHashMap[String, RecommendationEntry] =
    mutable.LinkedHashMap[String, RecommendationEntry]()
  // list of recommendations to be skipped for recommendations
  // Note that the recommendations will be computed anyway to avoid breaking dependencies.
  private val skippedRecommendations: mutable.HashSet[String] = mutable.HashSet[String]()
  // list of recommendations having the calculations disabled, and only depend on default values
  protected val limitedLogicRecommendations: mutable.HashSet[String] = mutable.HashSet[String]()
  // When enabled, the profiler recommendations should only include updated settings.
  private var filterByUpdatedPropertiesEnabled: Boolean = true

  private def isCalculationEnabled(prop: String) : Boolean = {
    !limitedLogicRecommendations.contains(prop)
  }

  def getPropertyValue(key: String): Option[String] = {
    val fromProfile = appInfoProvider.getProperty(key)
    // If the value is not found above, fallback to cluster properties
    fromProfile.orElse(Option(clusterProps.softwareProperties.get(key)))
  }

  def getAllProperties: collection.Map[String, String] = {
    // the app properties override the cluster properties
    clusterProps.getSoftwareProperties.asScala ++ appInfoProvider.getAllProperties
  }

  def initRecommendations(): Unit = {
    autoTunerConfigsProvider.recommendationsTarget.foreach { key =>
      // no need to add new records if they are missing from props
      getPropertyValue(key).foreach { propVal =>
        val recommendationVal = new RecommendationEntry(key, Option(propVal), None)
        recommendations(key) = recommendationVal
      }
    }
  }

  def appendRecommendation(key: String, value: String): Unit = {
    if (!skippedRecommendations.contains(key)) {
      val recomRecord = recommendations.getOrElseUpdate(key,
        new RecommendationEntry(key, getPropertyValue(key), None))
      if (value != null) {
        recomRecord.setRecommendedValue(value)
        if (recomRecord.original.isEmpty) {
          // add a comment that the value was missing in the cluster properties
          appendComment(s"'$key' was not set.")
        }
      }
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
    val gpuClusterRec = platform.getGPUInstanceTypeRecommendation(getAllProperties.toMap)
    if (gpuClusterRec.isDefined) {
      appendRecommendation("spark.executor.cores", gpuClusterRec.get.coresPerExecutor)
      if (gpuClusterRec.get.numExecutors > 0) {
        appendRecommendation("spark.executor.instances", gpuClusterRec.get.numExecutors)
      }
    }
  }

  def calcNumExecutorCores: Int = {
    val executorCores = platform.recommendedClusterInfo.map(_.coresPerExecutor).getOrElse(1)
    Math.max(1, executorCores)
  }

  /**
   * Recommendation for 'spark.task.resource.gpu.amount' based on num of cpu cores.
   */
  def calcTaskGPUAmount: Double = {
    val numExecutorCores = calcNumExecutorCores
    // can never be 0 since numExecutorCores has to be at least 1
    1.0 / numExecutorCores
  }

  /**
   * Recommendation for 'spark.rapids.sql.concurrentGpuTasks' based on gpu memory.
   * Assumption - cluster properties were updated to have a default values if missing.
   */
  def calcGpuConcTasks(): Long = {
    Math.min(autoTunerConfigsProvider.MAX_CONC_GPU_TASKS, platform.getGpuOrDefault.getGpuConcTasks)
  }

  /**
   * Calculates the available memory for each executor on the worker based on the number of
   * executors per node and the memory.
   * Assumption - cluster properties were updated to have a default values if missing.
   */
  private def calcAvailableMemPerExec(): Double = {
    val memMBPerNode = platform.recommendedNodeInstanceInfo.map(_.memoryMB).getOrElse(0L)
    val gpusPerExec = platform.getNumGPUsPerNode
    Math.max(0, memMBPerNode / gpusPerExec)
  }

  /**
   * Recommendation for initial heap size based on certain amount of memory per core.
   * Note that we will later reduce this if needed for off heap memory.
   */
  def calcInitialExecutorHeap(executorContainerMemCalculator: () => Double,
      numExecCores: Int): Long = {
    val maxExecutorHeap = Math.max(0, executorContainerMemCalculator()).toInt
    // give up to 2GB of heap to each executor core
    // TODO - revisit this in future as we could let heap be bigger
    Math.min(maxExecutorHeap, autoTunerConfigsProvider.DEF_HEAP_PER_CORE_MB * numExecCores)
  }

  /**
   * Recommendation of memory settings for executor.
   * Returns:
   * (pinned memory size,
   *  executor memory overhead size,
   *  executor heap size,
   *  boolean if should set MaxBytesInFlight)
   */
  def calcOverallMemory(
      execHeapCalculator: () => Long,
      numExecutorCores: Int,
      containerMemCalculator: () => Double): (Long, Long, Long, Boolean) = {
    val executorHeap = execHeapCalculator()
    val containerMem = containerMemCalculator.apply()
    var setMaxBytesInFlight = false
    // reserve 10% of heap as memory overhead
    var executorMemOverhead = (
      executorHeap * autoTunerConfigsProvider.DEF_HEAP_OVERHEAD_FRACTION
    ).toLong
    executorMemOverhead += autoTunerConfigsProvider.DEF_PAGEABLE_POOL_MB
    val containerMemLeftOverOffHeap = containerMem - executorHeap
    val minOverhead = executorMemOverhead + (
      autoTunerConfigsProvider.MIN_PINNED_MEMORY_MB + autoTunerConfigsProvider.MIN_SPILL_MEMORY_MB
    )
    logDebug("containerMem " + containerMem + " executorHeap: " + executorHeap +
      " executorMemOverhead: " + executorMemOverhead + " minOverhead " + minOverhead)
    if (containerMemLeftOverOffHeap >= minOverhead) {
      // this is hopefully path in the majority of cases because CSPs generally have a good
      // memory to core ratio
      if (numExecutorCores >= 16 && platform.isPlatformCSP &&
        containerMemLeftOverOffHeap >
          executorMemOverhead + 4096L + autoTunerConfigsProvider.MIN_PINNED_MEMORY_MB +
            autoTunerConfigsProvider.MIN_SPILL_MEMORY_MB) {
        // Account for the setting of:
        // appendRecommendation("spark.rapids.shuffle.multiThreaded.maxBytesInFlight", "4g")
        executorMemOverhead += 4096L
        setMaxBytesInFlight = true
      }
      // Pinned memory uses any unused space up to 4GB. Spill memory is same size as pinned.
      val pinnedMem = Math.min(autoTunerConfigsProvider.MAX_PINNED_MEMORY_MB,
        (containerMemLeftOverOffHeap - executorMemOverhead) / 2).toLong
      // Spill storage is set to the pinned size by default. Its not guaranteed to use just pinned
      // memory though so the size worst case would be doesn't use any pinned memory and uses
      // all off heap memory.
      val spillMem = pinnedMem
      if (containerMemLeftOverOffHeap >= executorMemOverhead + pinnedMem + spillMem) {
        executorMemOverhead += pinnedMem + spillMem
      } else {
        // use min pinned and spill mem
        executorMemOverhead += autoTunerConfigsProvider.MIN_PINNED_MEMORY_MB +
          autoTunerConfigsProvider.MIN_SPILL_MEMORY_MB
      }
      (pinnedMem, executorMemOverhead, executorHeap, setMaxBytesInFlight)
    } else {
      // otherwise we have to adjust heuristic of the executor heap size
      // recommendedMinHeap = DEF_HEAP_PER_CORE_MB * numExecutorCores
      // first calculate what we think min overhead is and make sure we have enough
      // for that
      // calculate minimum heap size
      val minExecHeapMem = autoTunerConfigsProvider.MIN_HEAP_PER_CORE_MB * numExecutorCores
      if ((containerMem - minOverhead) < minExecHeapMem) {
        // For now just throw so we don't get any tunings and its obvious to user this isn't a good
        // setup. In the future we may just recommend them to use larger nodes. This would be more
        // ideal once we hook up actual executor heap from an eventlog vs what user passes in.
        warnNotEnoughMem(minExecHeapMem + minOverhead)
        (0, 0, 0, false)
      } else {
        val leftOverMemUsingMinHeap = containerMem - minExecHeapMem
        if (leftOverMemUsingMinHeap < 0) {
          warnNotEnoughMem(minExecHeapMem + minOverhead)
        }
        // Pinned memory uses any unused space up to 4GB. Spill memory is same size as pinned.
        val pinnedMem = Math.min(autoTunerConfigsProvider.MAX_PINNED_MEMORY_MB,
          leftOverMemUsingMinHeap / 2).toLong
        val spillMem = pinnedMem
        // spill memory is by default same size as pinned memory
        executorMemOverhead += pinnedMem + spillMem
        (pinnedMem, executorMemOverhead, minExecHeapMem, setMaxBytesInFlight)
      }
    }
  }

  private def warnNotEnoughMem(minSize: Long): Unit = {
    // in the future it would be nice to enhance the error message with a recommendation of size
    val msg = "This node/worker configuration is not ideal for using the Spark Rapids\n" +
      "Accelerator because it doesn't have enough memory for the executors.\n" +
      s"We recommend using nodes/workers with more memory. Need at least ${minSize}MB memory."
    appendComment(msg)
  }

  /**
   * Find the label of the memory.overhead based on the spark master configuration and the spark
   * version.
   * @return "spark.executor.memoryOverhead", "spark.kubernetes.memoryOverheadFactor",
   *         or "spark.executor.memoryOverheadFactor".
   */
  def memoryOverheadLabel: String = {
    val sparkMasterConf = getPropertyValue("spark.master")
    val defaultLabel = "spark.executor.memoryOverhead"
    sparkMasterConf match {
      case None => defaultLabel
      case Some(sparkMaster) =>
        if (sparkMaster.contains("yarn")) {
          defaultLabel
        } else if (sparkMaster.contains("k8s")) {
          appInfoProvider.getSparkVersion match {
            case Some(version) =>
              if (ToolUtils.isSpark330OrLater(version)) {
                "spark.executor.memoryOverheadFactor"
              } else {
                "spark.kubernetes.memoryOverheadFactor"
              }
            case None => defaultLabel
          }
        } else {
          defaultLabel
        }
    }
  }

  /**
   * Flow:
   *   if "spark.master" is standalone => Do Nothing
   *   if "spark.rapids.memory.pinnedPool.size" is set
   *     if yarn -> recommend "spark.executor.memoryOverhead"
   *     if using k8s ->
   *         if version > 3.3.0 recommend "spark.executor.memoryOverheadFactor" and add comment
   *         else recommend "spark.kubernetes.memoryOverheadFactor" and add comment if missing
   */
  def addRecommendationForMemoryOverhead(recomValue: String): Unit = {
    if (autoTunerConfigsProvider
        .enableMemoryOverheadRecommendation(getPropertyValue("spark.master"))) {
      val memOverheadLookup = memoryOverheadLabel
      appendRecommendationForMemoryMB(memOverheadLookup, recomValue)
      getPropertyValue("spark.rapids.memory.pinnedPool.size").foreach { lookup =>
        if (lookup != "spark.executor.memoryOverhead") {
          if (getPropertyValue(memOverheadLookup).isEmpty) {
            appendComment(s"'$memOverheadLookup' must be set if using " +
              s"'spark.rapids.memory.pinnedPool.size")
          }
        }
      }
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
        appendRecommendation("spark.rapids.shuffle.multiThreaded.maxBytesInFlight", "4g")
      }
      appendRecommendation("spark.rapids.sql.reader.multithreaded.combine.sizeBytes",
        10 * 1024 * 1024)
      appendRecommendation("spark.rapids.sql.format.parquet.multithreaded.combine.waitTime", 1000)
    } else {
      val numThreads = (numExecutorCores * 2).toInt
      appendRecommendation("spark.rapids.sql.multiThreadedRead.numThreads",
        Math.max(20, numThreads).toInt)
      if (platform.isPlatformCSP) {
        if (setMaxBytesInFlight) {
          appendRecommendation("spark.rapids.shuffle.multiThreaded.maxBytesInFlight", "4g")
        }
        appendRecommendation("spark.rapids.sql.reader.multithreaded.combine.sizeBytes",
          10 * 1024 * 1024)
        appendRecommendation("spark.rapids.sql.format.parquet.multithreaded.combine.waitTime", 1000)
      }
    }
  }


  def calculateClusterLevelRecommendations(): Unit = {
    // only if we were able to figure out a node type to recommend do we make
    // specific recommendations
    if (platform.recommendedClusterInfo.isDefined) {
      val execCores = platform.recommendedClusterInfo.map(_.coresPerExecutor).getOrElse(1)
      appendRecommendation("spark.task.resource.gpu.amount", calcTaskGPUAmount)
      appendRecommendation("spark.rapids.sql.concurrentGpuTasks",
        calcGpuConcTasks().toInt)
      val availableMemPerExec = calcAvailableMemPerExec()
      val shouldSetMaxBytesInFlight = if (availableMemPerExec > 0.0) {
        val availableMemPerExecExpr = () => availableMemPerExec
        val executorHeap = calcInitialExecutorHeap(availableMemPerExecExpr, execCores)
        val executorHeapExpr = () => executorHeap
        val (pinnedMemory, memoryOverhead, finalExecutorHeap, setMaxBytesInFlight) =
          calcOverallMemory(executorHeapExpr, execCores, availableMemPerExecExpr)
        appendRecommendationForMemoryMB("spark.rapids.memory.pinnedPool.size", s"$pinnedMemory")
        addRecommendationForMemoryOverhead(s"$memoryOverhead")
        appendRecommendationForMemoryMB("spark.executor.memory", s"$finalExecutorHeap")
        setMaxBytesInFlight
      } else {
        logInfo("Available memory per exec is not specified")
        addMissingMemoryComments()
        false
      }
      configureShuffleReaderWriterNumThreads(execCores)
      configureMultiThreadedReaders(execCores, shouldSetMaxBytesInFlight)
      recommendAQEProperties()
    } else {
      addDefaultComments()
    }
    appendRecommendation("spark.rapids.sql.batchSizeBytes",
      autoTunerConfigsProvider.BATCH_SIZE_BYTES)
    appendRecommendation("spark.locality.wait", 0)
  }

  def calculateJobLevelRecommendations(): Unit = {
    // TODO - do we do anything with 200 shuffle partitions or maybe if its close
    // set the Spark config  spark.shuffle.sort.bypassMergeThreshold
    getShuffleManagerClassName match {
      case Right(smClassName) => appendRecommendation("spark.shuffle.manager", smClassName)
      case Left(comment) => appendComment(comment)
    }
    appendComment(autoTunerConfigsProvider.classPathComments("rapids.shuffle.jars"))
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
      getPropertyValue("spark.serializer") match {
        case Some(f) if f.contains("org.apache.spark.serializer.KryoSerializer") =>
          val existingRegistrars = getPropertyValue("spark.kryo.registrator")
          val regToUse = if (existingRegistrars.isDefined && !existingRegistrars.get.isEmpty) {
            // spark.kryo.registrator is a comma separated list. If the user set some then
            // we need to append our GpuKryoRegistrator to ones they specified.
            existingRegistrars.get + ",com.nvidia.spark.rapids.GpuKryoRegistrator"
          } else {
            "com.nvidia.spark.rapids.GpuKryoRegistrator"
          }
          appendRecommendation("spark.kryo.registrator", regToUse)
        case None =>
          // do nothing
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
            Right(autoTunerConfigsProvider.buildShuffleManagerClassName(smVersion))
          case None =>
            Left(autoTunerConfigsProvider.shuffleManagerCommentForUnsupportedVersion(sparkVersion))
        }
      case None =>
        Left(autoTunerConfigsProvider.shuffleManagerCommentForMissingVersion)
    }
  }

  /**
   * If the cluster worker-info is missing entries (i.e., CPU and GPU count), it sets the entries
   * to default values. For each default value, a comment is added to the [[comments]].
   */
  def configureClusterPropDefaults: Unit = {
    if (!clusterProps.system.isEmpty) {
      if (clusterProps.system.isMissingInfo) {
        clusterProps.system.setMissingFields(autoTunerConfigsProvider)
          .foreach(m => appendComment(m))
      }
      if (clusterProps.gpu.isMissingInfo) {
        clusterProps.gpu.setMissingFields(platform, autoTunerConfigsProvider)
          .foreach(m => appendComment(m))
      }
    }
  }

  private def recommendGCProperty(): Unit = {
    val jvmGCFraction = appInfoProvider.getJvmGCFractions
    if (jvmGCFraction.nonEmpty) { // avoid zero division
      if ((jvmGCFraction.sum / jvmGCFraction.size) >
        autoTunerConfigsProvider.MAX_JVM_GCTIME_FRACTION) {
        // TODO - or other cores/memory ratio
        appendComment("Average JVM GC time is very high. " +
          "Other Garbage Collectors can be used for better performance.")
      }
    }
  }

  private def recommendAQEProperties(): Unit = {
    val aqeEnabled = getPropertyValue("spark.sql.adaptive.enabled")
      .getOrElse("false").toLowerCase
    if (aqeEnabled == "false") {
      appendComment(autoTunerConfigsProvider.commentsForMissingProps("spark.sql.adaptive.enabled"))
    }
    appInfoProvider.getSparkVersion match {
      case Some(version) =>
        if (ToolUtils.isSpark320OrLater(version)) {
          // AQE configs changed in 3.2.0
          if (getPropertyValue("spark.sql.adaptive.coalescePartitions.minPartitionSize").isEmpty) {
            // the default is 1m, but 4m is slightly better for the GPU as we have a higher
            // per task overhead
            appendRecommendation("spark.sql.adaptive.coalescePartitions.minPartitionSize", "4m")
          }
        } else {
          if (getPropertyValue("spark.sql.adaptive.coalescePartitions.minPartitionNum").isEmpty) {
            // The ideal setting is for the parallelism of the cluster
            val numCoresPerExec = calcNumExecutorCores
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

    val advisoryPartitionSizeProperty =
      getPropertyValue("spark.sql.adaptive.advisoryPartitionSizeInBytes")
    if (appInfoProvider.getMeanInput <
      autoTunerConfigsProvider.AQE_INPUT_SIZE_BYTES_THRESHOLD) {
      if(advisoryPartitionSizeProperty.isEmpty) {
        // The default is 64m, but 128m is slightly better for the GPU as the GPU has sub-linear
        // scaling until it is full and 128m makes the GPU more full, but too large can be
        // slightly problematic because this is the compressed shuffle size
        appendRecommendation("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")
      }
    }
    if (appInfoProvider.getMeanInput > autoTunerConfigsProvider.AQE_INPUT_SIZE_BYTES_THRESHOLD &&
      appInfoProvider.getMeanShuffleRead >
        autoTunerConfigsProvider.AQE_SHUFFLE_READ_BYTES_THRESHOLD) {
      // AQE Recommendations for large input and large shuffle reads
      platform.getGpuOrDefault.getAdvisoryPartitionSizeInBytes.foreach { size =>
        appendRecommendation("spark.sql.adaptive.advisoryPartitionSizeInBytes", size)
      }
      val initialPartitionNumProperty =
        getPropertyValue("spark.sql.adaptive.coalescePartitions.initialPartitionNum").map(_.toInt)
      if (initialPartitionNumProperty.getOrElse(0) <=
            autoTunerConfigsProvider.AQE_MIN_INITIAL_PARTITION_NUM) {
        platform.getGpuOrDefault.getInitialPartitionNum.foreach { initialPartitionNum =>
          appendRecommendation(
            "spark.sql.adaptive.coalescePartitions.initialPartitionNum", initialPartitionNum)
        }
      }
      // We need to set this to false, else Spark ignores the target size specified by
      // spark.sql.adaptive.advisoryPartitionSizeInBytes.
      // Reference: https://spark.apache.org/docs/latest/sql-performance-tuning.html
      appendRecommendation("spark.sql.adaptive.coalescePartitions.parallelismFirst", "false")
    }

    // TODO - can we set spark.sql.autoBroadcastJoinThreshold ???
    val autoBroadcastJoinThresholdProperty =
      getPropertyValue("spark.sql.adaptive.autoBroadcastJoinThreshold").map(StringUtils.convertToMB)
    if (autoBroadcastJoinThresholdProperty.isEmpty) {
      appendComment("'spark.sql.adaptive.autoBroadcastJoinThreshold' was not set.")
    } else if (autoBroadcastJoinThresholdProperty.get >
        StringUtils.convertToMB(autoTunerConfigsProvider.AQE_AUTOBROADCAST_JOIN_THRESHOLD)) {
      appendComment("Setting 'spark.sql.adaptive.autoBroadcastJoinThreshold' > " +
        s"${autoTunerConfigsProvider.AQE_AUTOBROADCAST_JOIN_THRESHOLD} could " +
        s"lead to performance\n" +
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
    val missingRapidsJarsEntry = autoTunerConfigsProvider.classPathComments("rapids.jars.missing")
    val multipleRapidsJarsEntry = autoTunerConfigsProvider.classPathComments("rapids.jars.multiple")

    appInfoProvider.getRapidsJars match {
      case Seq() =>
        // No rapids jars
        appendComment(missingRapidsJarsEntry)
      case s: Seq[String] =>
        s.flatMap(e =>
          autoTunerConfigsProvider.pluginJarRegEx.findAllMatchIn(e).map(_.group(1))) match {
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
   * MIN_PARTITION_BYTES_RANGE = 128m, MAX_PARTITION_BYTES_RANGE = 256m
   * (1) Input:  maxPartitionBytes = 512m
   *             taskInputSize = 12m
   *     Output: newMaxPartitionBytes = 512m * (128m/12m) = 4g (hit max value)
   * (2) Input:  maxPartitionBytes = 2g
   *             taskInputSize = 512m,
   *     Output: newMaxPartitionBytes = 2g / (512m/128m) = 512m
   */
  private def calculateMaxPartitionBytes(maxPartitionBytes: String): String = {
    // AutoTuner only supports a single app right now, so we get whatever value is here
    val inputBytesMax = appInfoProvider.getMaxInput / 1024 / 1024
    val maxPartitionBytesNum = StringUtils.convertToMB(maxPartitionBytes)
    if (inputBytesMax == 0.0) {
      maxPartitionBytesNum.toString
    } else {
      if (inputBytesMax > 0 &&
        inputBytesMax < autoTunerConfigsProvider.MIN_PARTITION_BYTES_RANGE_MB) {
        // Increase partition size
        val calculatedMaxPartitionBytes = Math.min(
          maxPartitionBytesNum *
            (autoTunerConfigsProvider.MIN_PARTITION_BYTES_RANGE_MB / inputBytesMax),
          autoTunerConfigsProvider.MAX_PARTITION_BYTES_BOUND_MB)
        calculatedMaxPartitionBytes.toLong.toString
      } else if (inputBytesMax > autoTunerConfigsProvider.MAX_PARTITION_BYTES_RANGE_MB) {
        // Decrease partition size
        val calculatedMaxPartitionBytes = Math.min(
          maxPartitionBytesNum /
            (inputBytesMax / autoTunerConfigsProvider.MAX_PARTITION_BYTES_RANGE_MB),
          autoTunerConfigsProvider.MAX_PARTITION_BYTES_BOUND_MB)
        calculatedMaxPartitionBytes.toLong.toString
      } else {
        // Do not recommend maxPartitionBytes
        null
      }
    }
  }

  /**
   * Recommendation for 'spark.rapids.file.cache' based on read characteristics of job.
   */
  private def recommendFileCache() {
    if (appInfoProvider.getDistinctLocationPct <
          autoTunerConfigsProvider.DEF_DISTINCT_READ_THRESHOLD &&
        appInfoProvider.getRedundantReadSize >
          autoTunerConfigsProvider.DEF_READ_SIZE_THRESHOLD) {
      appendRecommendation("spark.rapids.filecache.enabled", "true")
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
        .getOrElse(autoTunerConfigsProvider.MAX_PARTITION_BYTES)
    val recommended =
      if (isCalculationEnabled("spark.sql.files.maxPartitionBytes")) {
        calculateMaxPartitionBytes(maxPartitionProp)
      } else {
        s"${StringUtils.convertToMB(maxPartitionProp)}"
      }
    appendRecommendationForMemoryMB("spark.sql.files.maxPartitionBytes", recommended)
  }

  /**
   * Recommendations for 'spark.sql.shuffle.partitions' based on spills and skew in shuffle stages.
   * Note that the logic can be disabled by adding the property to "limitedLogicRecommendations"
   * which is one of the arguments of [[getRecommendedProperties]].
   */
  def recommendShufflePartitions(): Unit = {
    val lookup = "spark.sql.shuffle.partitions"
    var shufflePartitions =
      getPropertyValue(lookup).getOrElse(autoTunerConfigsProvider.DEF_SHUFFLE_PARTITIONS).toInt

    // TODO: Need to look at other metrics for GPU spills (DEBUG mode), and batch sizes metric
    if (isCalculationEnabled(lookup)) {
      val shuffleStagesWithPosSpilling = appInfoProvider.getShuffleStagesWithPosSpilling
      if (shuffleStagesWithPosSpilling.nonEmpty) {
        val shuffleSkewStages = appInfoProvider.getShuffleSkewStages
        if (shuffleSkewStages.exists(id => shuffleStagesWithPosSpilling.contains(id))) {
          appendOptionalComment(lookup,
            "Shuffle skew exists (when task's Shuffle Read Size > 3 * Avg Stage-level size) in\n" +
            s"  stages with spilling. Increasing shuffle partitions is not recommended in this\n" +
            s"  case since keys will still hash to the same task.")
        } else {
           shufflePartitions *= autoTunerConfigsProvider.DEF_SHUFFLE_PARTITION_MULTIPLIER
          // Could be memory instead of partitions
          appendOptionalComment(lookup,
            s"'$lookup' should be increased since spilling occurred in shuffle stages.")
        }
      }
    }
    // If the user has enabled AQE auto shuffle, the auto-tuner should recommend to disable this
    // feature before recommending shuffle partitions.
    val aqeAutoShuffle = getPropertyValue("spark.databricks.adaptive.autoOptimizeShuffle.enabled")
    if (!aqeAutoShuffle.isEmpty) {
      appendRecommendation("spark.databricks.adaptive.autoOptimizeShuffle.enabled", "false")
    }
    appendRecommendation("spark.sql.shuffle.partitions", s"$shufflePartitions")
  }

  /**
   * Analyzes unsupported driver logs and generates recommendations for configuration properties.
   */
  private def recommendFromDriverLogs(): Unit = {
    // Iterate through unsupported operators' reasons and check for matching properties
    driverInfoProvider.getUnsupportedOperators.map(_.reason).foreach { operatorReason =>
      autoTunerConfigsProvider.recommendationsFromDriverLogs.collect {
        case (config, recommendedValue) if operatorReason.contains(config) =>
          appendRecommendation(config, recommendedValue)
          appendComment(autoTunerConfigsProvider.commentForExperimentalConfig(config))
      }
    }
  }

  private def recommendPluginProps(): Unit = {
    val isPluginLoaded = getPropertyValue("spark.plugins") match {
      case Some(f) => f.contains("com.nvidia.spark.SQLPlugin")
      case None => false
    }
    val rapidsEnabled = getPropertyValue("spark.rapids.sql.enabled") match {
      case Some(f) => f.toBoolean
      case None => true
    }
    if (!rapidsEnabled) {
      appendRecommendation("spark.rapids.sql.enabled", "true")
    }
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
    autoTunerConfigsProvider.commentsForMissingProps.foreach {
      case (key, value) =>
        if (!skippedRecommendations.contains(key)) {
          appendComment(value)
        }
    }
  }

  private def addMissingMemoryComments(): Unit = {
    autoTunerConfigsProvider.commentsForMissingMemoryProps.foreach {
      case (key, value) =>
        if (!skippedRecommendations.contains(key)) {
          appendComment(value)
        }
    }
  }

  private def toCommentProfileResult: Seq[RecommendedCommentResult] = {
    comments.map(RecommendedCommentResult).sortBy(_.comment)
  }

  private def toRecommendationsProfileResult: Seq[RecommendedPropertyResult] = {
    val finalRecommendations =
      recommendations.filter(elem => elem._2.isValid(filterByUpdatedPropertiesEnabled))
    finalRecommendations.collect {
      case (key, record) => RecommendedPropertyResult(key, record.recommended.get)
    }.toSeq.sortBy(_.property)
  }

  /**
   * The Autotuner loads the spark properties from either the ClusterProperties or the eventlog.
   * 1- runs the calculation for each criterion and saves it as a [[RecommendationEntry]].
   * 2- The final list of recommendations include any [[RecommendationEntry]] that has a
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
      (Seq[RecommendedPropertyResult], Seq[RecommendedCommentResult]) = {
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
        platform.setNumGpus(clusterProps.gpu.getCount)
      }
      // configured GPU recommended instance type NEEDS to happen before any of the other
      // recommendations as they are based on
      // the instance type
      configureGPURecommendedInstanceType
      configureClusterPropDefaults
      // Makes recommendations based on information extracted from the AppInfoProvider
      filterByUpdatedPropertiesEnabled = showOnlyUpdatedProps
      recommendPluginProps
      calculateJobLevelRecommendations()
      calculateClusterLevelRecommendations()

      // add all platform specific recommendations
      platform.recommendationsToInclude.foreach {
        case (property, value) => appendRecommendation(property, value)
      }
    }
    recommendFromDriverLogs()
    (toRecommendationsProfileResult, toCommentProfileResult)
  }

  // Process the properties keys. This is needed in case there are some properties that should not
  // be listed in the final combined results. For example:
  // - The UUID of the app is not part of the submitted spark configurations
  // - make sure that we exclude the skipped list
  private def processPropKeys(
      srcMap: collection.Map[String, String]): collection.Map[String, String] = {
    (srcMap -- skippedRecommendations) -- autoTunerConfigsProvider.filteredPropKeys
  }

  // Combines the original Spark properties with the recommended ones.
  def combineSparkProperties(
      recommendedSet: Seq[RecommendedPropertyResult]): Seq[RecommendedPropertyResult] = {
    // get the original properties after filtering the and removing unnecessary keys
    val originalPropsFiltered = processPropKeys(getAllProperties)
    // Combine the original properties with the recommended properties.
    // The recommendations should always override the original ones
    val combinedProps = (originalPropsFiltered
      ++ recommendedSet.map(r => r.property -> r.value).toMap).toSeq.sortBy(_._1)
    combinedProps.collect {
      case (pK, pV) => RecommendedPropertyResult(pK, pV)
    }
  }
}

/**
 * Trait defining configuration defaults and parameters for the AutoTuner.
 */
trait AutoTunerConfigsProvider extends Logging {
  // Maximum number of concurrent tasks to run on the GPU
  val MAX_CONC_GPU_TASKS = 4L
  // Amount of CPU memory to reserve for system overhead (kernel, buffers, etc.) in megabytes
  val DEF_SYSTEM_RESERVE_MB: Long = 2 * 1024L
  // Fraction of the executor JVM heap size that should be additionally reserved
  // for JVM off-heap overhead (thread stacks, native libraries, etc.)
  val DEF_HEAP_OVERHEAD_FRACTION = 0.1
  val MAX_JVM_GCTIME_FRACTION = 0.3
  // Minimum amount of JVM heap memory to request per CPU core in megabytes
  val MIN_HEAP_PER_CORE_MB: Long = 750L
  // Ideal amount of JVM heap memory to request per CPU core in megabytes
  val DEF_HEAP_PER_CORE_MB: Long = 2 * 1024L
  // Minimum amount of pinned memory to use per executor in MB
  val MIN_PINNED_MEMORY_MB: Long = 1024L
  val MIN_SPILL_MEMORY_MB: Long = MIN_PINNED_MEMORY_MB
  // Maximum amount of pinned memory to use per executor in MB
  val MAX_PINNED_MEMORY_MB: Long = 4 * 1024L
  // Default pinned memory to use per executor in MB
  val DEF_PINNED_MEMORY_MB: Long = 2 * 1024L
  // the pageable pool doesn't exist anymore but by default we don't have any hard limits so
  // leave this for now to account for off heap memory usage.
  val DEF_PAGEABLE_POOL_MB: Long = 2 * 1024L
  // value in MB
  val MIN_PARTITION_BYTES_RANGE_MB = 128L
  // value in MB
  val MAX_PARTITION_BYTES_RANGE_MB = 256L
  // value in MB
  val MAX_PARTITION_BYTES_BOUND_MB: Int = 4 * 1024
  val MAX_PARTITION_BYTES: String = "512m"
  val DEF_SHUFFLE_PARTITIONS = "200"
  val DEF_SHUFFLE_PARTITION_MULTIPLIER: Int = 2
  // GPU count defaults to 1 if it is missing.
  val DEF_WORKER_GPU_COUNT = 1
  // Default Number of Workers 1
  val DEF_NUM_WORKERS = 1
  // Default distinct read location thresholds is 50%
  val DEF_DISTINCT_READ_THRESHOLD = 50.0
  // Default file cache size minimum is 100 GB
  val DEF_READ_SIZE_THRESHOLD = 100 * 1024L * 1024L * 1024L
  val DEFAULT_WORKER_INFO_PATH = "./worker_info.yaml"
  val SUPPORTED_SIZE_UNITS: Seq[String] = Seq("b", "k", "m", "g", "t", "p")
  private val DOC_URL: String = "https://nvidia.github.io/spark-rapids/docs/" +
    "additional-functionality/advanced_configs.html#advanced-configuration"
  // Value of batchSizeBytes that performs best overall
  val BATCH_SIZE_BYTES = 2147483647
  val AQE_INPUT_SIZE_BYTES_THRESHOLD = 35000
  val AQE_SHUFFLE_READ_BYTES_THRESHOLD = 50000
  val AQE_MIN_INITIAL_PARTITION_NUM = 200
  val AQE_AUTOBROADCAST_JOIN_THRESHOLD = "100m"
  // Set of spark properties to be filtered out from the combined Spark properties.
  val filteredPropKeys: Set[String] = Set(
    "spark.app.id"
  )

  val commentsForMissingMemoryProps: Map[String, String] = Map(
    "spark.executor.memory" ->
      "'spark.executor.memory' should be set to at least 2GB/core.",
    "spark.rapids.memory.pinnedPool.size" ->
      s"'spark.rapids.memory.pinnedPool.size' should be set to ${DEF_PINNED_MEMORY_MB}m.")

  val commentsForMissingProps: Map[String, String] = Map(
    "spark.executor.instances" ->
      "'spark.executor.instances' should be set to (gpuCount * numWorkers).",
    "spark.task.resource.gpu.amount" ->
      "'spark.task.resource.gpu.amount' should be set to Min(1, (gpuCount / numCores)).",
    "spark.rapids.sql.concurrentGpuTasks" ->
      s"'spark.rapids.sql.concurrentGpuTasks' should be set to Min(4, (gpuMemory / 7.5G)).",
    "spark.rapids.sql.enabled" ->
      "'spark.rapids.sql.enabled' should be true to enable SQL operations on the GPU.",
    "spark.sql.adaptive.enabled" ->
      "'spark.sql.adaptive.enabled' should be enabled for better performance."
  ) ++ commentsForMissingMemoryProps

  val recommendationsTarget: Seq[String] = Seq[String](
    "spark.executor.instances",
    "spark.rapids.sql.enabled",
    "spark.executor.cores",
    "spark.executor.memory",
    "spark.rapids.sql.concurrentGpuTasks",
    "spark.task.resource.gpu.amount",
    "spark.sql.shuffle.partitions",
    "spark.sql.files.maxPartitionBytes",
    "spark.rapids.memory.pinnedPool.size",
    "spark.executor.memoryOverhead",
    "spark.executor.memoryOverheadFactor",
    "spark.kubernetes.memoryOverheadFactor")

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

  // Recommended values for specific unsupported configurations
  val recommendationsFromDriverLogs: Map[String, String] = Map(
    "spark.rapids.sql.incompatibleDateFormats.enabled" -> "true"
  )

  def commentForExperimentalConfig(config: String): String = {
    s"Using $config does not guarantee to produce the same results as CPU. " +
      s"Please refer to $DOC_URL."
  }

  // the plugin jar is in the form of rapids-4-spark_scala_binary-(version)-*.jar
  val pluginJarRegEx: Regex = "rapids-4-spark_\\d\\.\\d+-(\\d{2}\\.\\d{2}\\.\\d+).*\\.jar".r

  private val shuffleManagerDocUrl = "https://docs.nvidia.com/spark-rapids/user-guide/latest/" +
    "additional-functionality/rapids-shuffle.html#rapids-shuffle-manager"

  /**
   * Abstract method to create an instance of the AutoTuner.
   */
  def createAutoTunerInstance(
    clusterProps: ClusterProperties,
    appInfoProvider: AppSummaryInfoBaseProvider,
    platform: Platform,
    driverInfoProvider: DriverLogInfoProvider): AutoTuner

  def handleException(
      ex: Throwable,
      appInfo: AppSummaryInfoBaseProvider,
      platform: Platform,
      driverInfoProvider: DriverLogInfoProvider): AutoTuner = {
    logError("Exception: " + ex.getStackTrace.mkString("Array(", ", ", ")"))
    val tuning = createAutoTunerInstance(new ClusterProperties(), appInfo,
      platform, driverInfoProvider)
    val msg = ex match {
      case cEx: ConstructorException => cEx.getContext
      case _ => if (ex.getCause != null) ex.getCause.toString else ex.toString
    }
    tuning.appendComment(msg)
    tuning
  }

  def loadClusterPropertiesFromContent(clusterProps: String): Option[ClusterProperties] = {
    val representer = new Representer(new DumperOptions())
    representer.getPropertyUtils.setSkipMissingProperties(true)
    val constructor = new Constructor(classOf[ClusterProperties], new LoaderOptions())
    val yamlObjNested = new Yaml(constructor, representer)
    val loadedClusterProps = yamlObjNested.load(clusterProps).asInstanceOf[ClusterProperties]
    if (loadedClusterProps != null && loadedClusterProps.softwareProperties == null) {
      logInfo("softwareProperties is empty from input worker_info file")
      loadedClusterProps.softwareProperties = new util.LinkedHashMap[String, String]()
    }
    Option(loadedClusterProps)
  }

  def loadClusterProps(filePath: String): Option[ClusterProperties] = {
    val path = new Path(filePath)
    var fsIs: FSDataInputStream = null
    try {
      val fs = FileSystem.get(path.toUri, new Configuration())
      fsIs = fs.open(path)
      val reader = new BufferedReader(new InputStreamReader(fsIs))
      val fileContent = Stream.continually(reader.readLine()).takeWhile(_ != null).mkString("\n")
      loadClusterPropertiesFromContent(fileContent)
    } catch {
      // In case of missing file/malformed for cluster properties, default properties are used.
      // Hence, catching and logging as a warning
      case _: IOException =>
        logWarning(s"No file found for input workerInfo path: $filePath")
        None
    } finally {
      if (fsIs != null) {
        fsIs.close()
      }
    }
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
      driverInfoProvider: DriverLogInfoProvider = BaseDriverLogInfoProvider.noneDriverLog
  ): AutoTuner = {
    try {
      val clusterPropsOpt = loadClusterPropertiesFromContent(clusterProps)
      createAutoTunerInstance(clusterPropsOpt.getOrElse(new ClusterProperties()),
        singleAppProvider, platform, driverInfoProvider)
    } catch {
      case NonFatal(e) =>
        handleException(e, singleAppProvider, platform, driverInfoProvider)
    }
  }

  def buildAutoTuner(
      singleAppProvider: AppSummaryInfoBaseProvider,
      platform: Platform,
      driverInfoProvider: DriverLogInfoProvider = BaseDriverLogInfoProvider.noneDriverLog
  ): AutoTuner = {
    try {
      val autoT = createAutoTunerInstance(
        platform.clusterProperties.getOrElse(new ClusterProperties()),
        singleAppProvider, platform, driverInfoProvider)
      autoT
    } catch {
      case NonFatal(e) =>
        handleException(e, singleAppProvider, platform, driverInfoProvider)
    }
  }

  /**
   * Given the spark property "spark.master", it checks whether memoryOverhead should be
   * enabled/disabled. For Spark Standalone Mode, memoryOverhead property is skipped.
   * @param confValue the value of property "spark.master"
   * @return False if the value is a spark standalone. True if the value is not defined or
   *         set for yarn/Mesos
   */
  def enableMemoryOverheadRecommendation(confValue: Option[String]): Boolean = {
    confValue match {
      case Some(sparkMaster) if sparkMaster.startsWith("spark:") => false
      case _ => true
    }
  }

  def buildShuffleManagerClassName(smVersion: String): String = {
    s"com.nvidia.spark.rapids.spark$smVersion.RapidsShuffleManager"
  }

  def shuffleManagerCommentForUnsupportedVersion(sparkVersion: String): String = {
    s"Cannot recommend RAPIDS Shuffle Manager for unsupported \'$sparkVersion\' version.\n" +
      s"  See supported versions: $shuffleManagerDocUrl."
  }

  def shuffleManagerCommentForMissingVersion: String = {
    "Could not recommend RapidsShuffleManager as Spark version cannot be determined."
  }
}

/**
 * Provides configuration settings for the Profiling Tool's AutoTuner. This object is as a concrete
 * implementation of the `AutoTunerConfigsProvider` interface.
 */
object ProfilingAutoTunerConfigsProvider extends AutoTunerConfigsProvider {
  def createAutoTunerInstance(
      clusterProps: ClusterProperties,
      appInfoProvider: AppSummaryInfoBaseProvider,
      platform: Platform,
      driverInfoProvider: DriverLogInfoProvider): AutoTuner = {
    new AutoTuner(clusterProps, appInfoProvider, platform, driverInfoProvider,
      ProfilingAutoTunerConfigsProvider)
  }
}
