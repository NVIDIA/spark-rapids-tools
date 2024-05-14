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

package com.nvidia.spark.rapids.tool.views

import com.nvidia.spark.rapids.tool.analysis.{ProfAppIndexMapperTrait, QualAppIndexMapperTrait}
import com.nvidia.spark.rapids.tool.profiling.{BlockManagerRemovedProfileResult, ExecutorInfoProfileResult, ExecutorsRemovedProfileResult}

import org.apache.spark.resource.ResourceProfile
import org.apache.spark.sql.rapids.tool.AppBase


trait AppExecutorViewTrait extends ViewableTrait[ExecutorInfoProfileResult] {
  override def getLabel: String = "Executor Information"

  override def getRawView(app: AppBase, index: Int): Seq[ExecutorInfoProfileResult] = {
    // first see if any executors have different resourceProfile ids
    val groupedExecs = app.executorIdToInfo.groupBy(_._2.resourceProfileId)
    groupedExecs.map { case (rpId, execs) =>
      val rp = app.resourceProfIdToInfo.get(rpId)
      val execMem = rp.map(_.executorResources.get(ResourceProfile.MEMORY)
        .map(_.amount).getOrElse(0L))
      val execGpus = rp.map(_.executorResources.get("gpu")
        .map(_.amount).getOrElse(0L))
      val taskCpus = rp.map(_.taskResources.get(ResourceProfile.CPUS)
        .map(_.amount).getOrElse(0.toDouble))
      val taskGpus = rp.map(_.taskResources.get("gpu").map(_.amount).getOrElse(0.toDouble))
      val execOffHeap = rp.map(_.executorResources.get(ResourceProfile.OFFHEAP_MEM)
        .map(_.amount).getOrElse(0L))

      val numExecutors = execs.size
      val exec = execs.head._2
      // We could print a lot more information here if we decided, more like the Spark UI
      // per executor info.
      ExecutorInfoProfileResult(index, rpId, numExecutors,
        exec.totalCores, exec.maxMemory, exec.totalOnHeap,
        exec.totalOffHeap, execMem, execGpus, execOffHeap, taskCpus, taskGpus)
    }.toSeq
  }

  override def sortView(rows: Seq[ExecutorInfoProfileResult]): Seq[ExecutorInfoProfileResult] = {
    rows.sortBy(cols => (cols.appIndex, cols.resourceProfileId))
  }
}

trait AppRemovedExecutorView extends ViewableTrait[ExecutorsRemovedProfileResult] {
  override def getLabel: String = "Removed Executors"

  override def getRawView(app: AppBase, index: Int): Seq[ExecutorsRemovedProfileResult] = {
    val execsRemoved = app.executorIdToInfo.filter { case (_, exec) =>
      !exec.isActive
    }
    execsRemoved.map { case (id, exec) =>
      ExecutorsRemovedProfileResult(index, id, exec.removeTime, exec.removeReason)
    }.toSeq
  }

  override def sortView(
      rows: Seq[ExecutorsRemovedProfileResult]): Seq[ExecutorsRemovedProfileResult] = {
    rows.sortBy(cols => (cols.appIndex, cols.executorId))
  }
}

trait AppRemovedBlockManagerView extends ViewableTrait[BlockManagerRemovedProfileResult] {
  override def getLabel: String = "Removed BlockManagers"

  override def getRawView(app: AppBase, index: Int): Seq[BlockManagerRemovedProfileResult] = {
    app.blockManagersRemoved.map { bm =>
      BlockManagerRemovedProfileResult(index, bm.executorId, bm.time)
    }
  }

  override def sortView(
      rows: Seq[BlockManagerRemovedProfileResult]): Seq[BlockManagerRemovedProfileResult] = {
    rows.sortBy(cols => (cols.appIndex, cols.executorId))
  }
}

object QualExecutorView extends AppExecutorViewTrait with QualAppIndexMapperTrait {
  // Keep for the following refactor stages to customize the view based on the app type (Qual/Prof)
}

object ProfExecutorView extends AppExecutorViewTrait with ProfAppIndexMapperTrait {
  // Keep for the following refactor stages to customize the view based on the app type (Qual/Prof)
}

object QualRemovedExecutorView extends AppRemovedExecutorView with QualAppIndexMapperTrait {
  // Keep for the following refactor stages to customize the view based on the app type (Qual/Prof)
}

object ProfRemovedExecutorView extends AppRemovedExecutorView with ProfAppIndexMapperTrait {
  // Keep for the following refactor stages to customize the view based on the app type (Qual/Prof)
}

object QualRemovedBLKMgrView extends AppRemovedBlockManagerView with QualAppIndexMapperTrait {
  // Keep for the following refactor stages to customize the view based on the app type (Qual/Prof)
}

object ProfRemovedBLKMgrView extends AppRemovedBlockManagerView with ProfAppIndexMapperTrait {
  // Keep for the following refactor stages to customize the view based on the app type (Qual/Prof)
}
