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

package org.apache.spark.sql.rapids.tool

import org.apache.spark.resource.{ResourceInformation, ResourceProfile}

class ExecutorInfoClass(val executorId: String, _addTime: Long) {
    var hostPort: String = null
    var host: String = null
    var isActive = true
    var totalCores = 0

    val addTime = _addTime
    var removeTime: Long = 0L
    var removeReason: String = null
    var maxMemory = 0L

    var resources = Map[String, ResourceInformation]()

    // Memory metrics. They may not be recorded (e.g. old event logs) so if totalOnHeap is not
    // initialized, the store will not contain this information.
    var totalOnHeap = -1L
    var totalOffHeap = 0L

    var resourceProfileId = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID
}

sealed trait ClusterInfo {
    def vendor: String
    def coresPerExecutor: Int
    def numExecsPerNode: Int
    def numWorkerNodes: Int
    def driverNodeType: Option[String]
    def workerNodeType: Option[String]
}

// Information about the cluster used with to run the application we
// are qualifying or profiling. This is compared to what we might recommend
// for a cluster.
case class ExistingClusterInfo(
    vendor: String,
    coresPerExecutor: Int,
    numExecsPerNode: Int,
    numExecutors: Int, // note that with dynamic allocation  this is high water mark
    numWorkerNodes: Int,
    executorHeapMemory: Long,
    dynamicAllocationEnabled: Boolean,
    dynamicAllocationMaxExecutors: String,
    dynamicAllocationMinExecutors: String,
    dynamicAllocationInitialExecutors: String,
    driverNodeType: Option[String] = None,
    workerNodeType: Option[String] = None,
    driverHost: Option[String] = None,
    clusterId: Option[String] = None,
    clusterName: Option[String] = None) extends ClusterInfo

case class RecommendedClusterInfo(
    vendor: String,
    coresPerExecutor: Int,
    numWorkerNodes: Int,
    numGpus: Int,
    numExecutors: Int,
    gpuDevice: String,
    dynamicAllocationEnabled: Boolean,
    dynamicAllocationMaxExecutors: String,
    dynamicAllocationMinExecutors: String,
    dynamicAllocationInitialExecutors: String,
    driverNodeType: Option[String] = None,
    workerNodeType: Option[String] = None) extends ClusterInfo {
    // The number of executors per node is the same as the number of GPUs
    def numExecsPerNode: Int = numGpus
}

case class ClusterSummary(
    appName: String,
    appId: String,
    eventLogPath: Option[String],
    clusterInfo: Option[ExistingClusterInfo],
    recommendedClusterInfo: Option[RecommendedClusterInfo])
