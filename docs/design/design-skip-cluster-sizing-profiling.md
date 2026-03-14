<!--
  Copyright (c) 2026, NVIDIA CORPORATION.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Design: Skip Cluster Sizing Recommendations in Profiling Mode

## Background

The AutoTuner has two modes:
- **Qualification**: Analyzes CPU event logs and recommends a GPU cluster configuration (new hardware). Cluster sizing recommendations are essential.
- **Profiling**: Analyzes GPU event logs from an existing deployment and tunes GPU-specific parameters. Cluster sizing recommendations are not actionable since hardware is already in place.

Currently, both modes produce cluster sizing recommendations. This design removes sizing recommendations from profiling mode.

## Key Decisions

### 1. Scope: On-prem only

The reasoning "hardware is already purchased" applies most strongly to on-prem deployments where customers have already bought their cluster hardware. For CSP (cloud) platforms (Dataproc, EMR, Databricks), the tool has instance type catalogs and sizing recommendations remain actionable since cloud resources can be reconfigured. Narrowing the scope to on-prem avoids breaking the 78+ existing CSP profiling tests that expect full sizing recommendations.

### 2. Target cluster override

If the user explicitly provides a `targetCluster` with `workerInfo`, they are asking the tool to size their job for specific target hardware. In this case, full recommendations apply even in on-prem profiling mode. The condition is:

```
skipSizing = isProfilingMode && !isPlatformCSP && targetCluster.isEmpty
```

### 3. What to skip vs. keep

**Skip** (node/container shape — requires hardware or cluster changes):
- `spark.executor.cores`, `spark.executor.memory`, `spark.executor.memoryOverhead`
- `spark.executor.instances`
- `spark.dynamicAllocation.{initial,min,max}Executors`

**Keep** (GPU tuning within existing container — software-tunable):
- `spark.rapids.memory.pinnedPool.size`, `spark.memory.offHeap.{size,enabled}`, `spark.rapids.memory.host.offHeapLimit.size`
- `spark.rapids.sql.concurrentGpuTasks`, `spark.rapids.sql.batchSizeBytes`
- `spark.task.resource.gpu.amount`, `spark.executor.resource.gpu.*`
- Shuffle threads, multiThreadedReaders
- `spark.locality.wait`
- All job-level recommendations (shuffle manager, file cache, partitions, AQE, kryo, GC, plugins)

Rationale: GPU memory settings like pinnedPool and off-heap are tunable without changing the node. Users may have misconfigured these, and the tool should still correct them.

## Approach: Override in ProfilingAutoTuner + Extract Protected Helpers

### Refactor base AutoTuner

Split the monolithic `calculateClusterLevelRecommendations()` method into 3 composable protected methods:

| Method | Responsibility | Skipped in profiling? |
|---|---|---|
| `recommendGpuComputeSettings()` | GPU task resources, concurrentGpuTasks | No |
| `recommendGpuMemorySettings(execCores)` | pinnedPool, off-heap, host.offHeapLimit | No |
| `recommendShuffleAndReaderSettings(execCores)` | shuffle threads, multiThreadedReaders | No |
| `recommendAQEProperties()` | AQE partition tuning, advisory sizes, broadcast thresholds | No |
| `recommendNodeSizingSettings(execCores)` | executor.memory, memoryOverhead, executor cores/instances | Yes |
| `recommendClusterScalingSettings(execCores)` | dynamic allocation (min/initial/max executors) | Yes |

The base `calculateClusterLevelRecommendations()` calls all four. `batchSizeBytes` and `locality.wait` are always appended (outside the cluster info block).

Also change `configureGPURecommendedInstanceType()` from `private` to `protected`.

### Override in ProfilingAutoTuner

```
configureGPURecommendedInstanceType():
  if CSP or targetCluster is provided → delegate to super (full behavior)
  else (on-prem, no target) → still compute cluster info but don't emit cores/instances

calculateClusterLevelRecommendations():
  if CSP or targetCluster is provided → delegate to super (full behavior)
  else (on-prem, no target) → call recommendGpuComputeSettings() + recommendGpuMemorySettingsOnly()
         + recommendShuffleAndReaderSettings() + recommendAQEProperties()
         skip recommendClusterScalingSettings() (dynamic allocation)
```

### Why this approach

- **No new strategy classes**: The existing `ConstantGpuCountStrategy` continues to work. We just don't emit its sizing results.
- **Clean separation**: Profiling-specific behavior lives in `ProfilingAutoTuner`, not scattered across the base class.
- **Backward compatible**: CSP platforms and `targetCluster` override ensure existing behavior is preserved.
- **Minimal risk**: Qualification mode is completely untouched.

## Files to Modify

| File | Change |
|---|---|
| `core/src/main/scala/.../tuning/AutoTuner.scala` | Extract 3 protected helpers; make `configureGPURecommendedInstanceType` protected |
| `core/src/main/scala/.../tuning/AutoTuner.scala` (ProfilingAutoTuner) | Override 2 methods with `targetCluster` gating |
| `core/src/test/scala/.../tuning/ProfilingAutoTunerSuite.scala` | Add tests for with/without target cluster |

## Testing

1. **Profiling without target cluster**: Verify sizing properties absent, GPU properties present
2. **Profiling with target cluster**: Verify full recommendations (existing behavior)
3. **Qualification mode**: Verify no regression (sizing always recommended)
4. **Existing test suites**: ProfilingAutoTunerSuite, QualificationAutoTunerSuite, BaseAutoTunerSuite all pass
