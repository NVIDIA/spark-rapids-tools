# Lightweight Event Log Runtime Detector — Design

- **Issue:** [NVIDIA/spark-rapids-tools#2082](https://github.com/NVIDIA/spark-rapids-tools/issues/2082)
- **Branch:** `sbari-issue-2082`
- **Date:** 2026-04-22
- **Status:** Design approved, ready for implementation plan

## 1. Problem

Today, determining whether a Spark event log came from a CPU or GPU run requires running the full qualification or profiling tool. That is expensive and wrong way to use the tools for the pre-flight decision of *which* tool to run.

aether (a consumer of spark-rapids-tools) needs to make this decision per job before invoking either tool. Its current workaround is running one tool, reading `application_information.csv`'s `sparkRuntime` column, and picking the right tool — meaning every detection costs a full qualification run.

## 2. Goal

Expose a lightweight Python function that inspects the first handful of events in a Spark event log and returns the runtime classification (`SPARK`, `SPARK_RAPIDS`, `PHOTON`, `AURON`) without invoking the Scala tools pipeline.

## 3. Non-goals (V1)

- Replacing the Scala event log reader. The Scala reader handles multi-app directories, wildcards, comma-separated lists, malformed logs, and every CSP quirk. The Python detector is a deliberately narrow shortcut for the common single-app case.
- Public user-facing CLI. This is a dev/library-scope feature, not part of the supported `spark_rapids_user_tools` CLI surface.
- Writing any output files. Result is a Python object returned in-process.

## 4. Consumers

- **Primary:** aether (`aether-services`). aether already consumes `spark_rapids_tools.cmdli.tools_cli.ToolsCLI` as a library. It will import `detect_spark_runtime` directly, call it before deciding to run qualification or profiling, and branch on the `spark_runtime` value — which matches the strings it already stores in `JobRun.spark_runtime`.
- **Secondary:** internal scripting / debugging. Devs can `python -c "..."` to inspect a log.

## 5. Public API

Module: `user_tools/src/spark_rapids_tools/tools/eventlog_detector.py`.

```python
from enum import Enum
from dataclasses import dataclass
from typing import Optional, Union
from spark_rapids_tools.storagelib.cspfs import BoundedCspPath

class SparkRuntime(str, Enum):
    SPARK = "SPARK"
    SPARK_RAPIDS = "SPARK_RAPIDS"
    PHOTON = "PHOTON"
    AURON = "AURON"

@dataclass(frozen=True)
class RuntimeInfo:
    spark_runtime: SparkRuntime
    app_id: Optional[str]          # None if SparkListenerApplicationStart not seen
    app_name: Optional[str]
    spark_version: Optional[str]   # from SparkListenerLogStart
    event_log_path: str            # concrete file actually read
    source_path: str               # caller input (preserves rolling-dir input)

def detect_spark_runtime(
    event_log: Union[str, BoundedCspPath],
    *,
    max_events_scanned: int = 1000,
) -> RuntimeInfo: ...
```

Enum string values match the Scala `SparkRuntime` enum exactly so aether's existing string comparisons (`JobRun.spark_runtime != 'SPARK_RAPIDS'`) work unchanged.

## 6. Scope of input shapes (V1)

| Input shape | Supported | Notes |
| --- | --- | --- |
| Single file (plain or compressed) | ✅ | Primary case |
| Spark native rolling dir (`eventlog_v2_*`) | ✅ | Reads first (earliest) `events_1_*` chunk — it holds `ApplicationStart` + `EnvironmentUpdate` |
| Databricks rolling dir (`eventlog`, `eventlog-*` files) | ✅ | Sort mirroring Scala's `DatabricksRollingEventLogFilesFileReader` (see 7.1), read the earliest |
| Generic directory of independent logs | ❌ | Raises `UnsupportedEventLogShapeError` |
| Wildcard path | ❌ | Same |
| Comma-separated list | ❌ | Same |

Rationale: aether's `is_multi_event_log_input()` already distinguishes single-app from multi-app inputs. Pre-flight detection is most useful for the single-app case; multi-app inputs don't bind to a single `aether_job_id` anyway.

Compression codecs (Spark-native set per `EventLogPathProcessor.SPARK_SHORT_COMPRESSION_CODEC_NAMES` — `lz4, lzf, snappy, zstd`, plus `gz`):

| Extension | Dependency | Availability |
| --- | --- | --- |
| none, `.inprogress` | stdlib | always |
| `.gz` | stdlib `gzip` | always |
| `.zstd`, `.zst` | `zstandard` | **non-optional new dep** — existing test fixtures are zstd; default in several Spark deployments |
| `.lz4` | `lz4` | optional extra `spark-rapids-user-tools[compression]` (new extra, introduced in this PR) |
| `.lzf` | `python-lzf` (or equivalent — plan will pin the choice) | optional extra `spark-rapids-user-tools[compression]` |
| `.snappy` | `cramjam` | optional extra `spark-rapids-user-tools[compression]` |

Missing optional codec lib → `UnsupportedCompressionError` with actionable install message pointing at the correct PyPI name `spark-rapids-user-tools[compression]`. The `[compression]` extra is introduced by this feature; it does not exist in `user_tools/pyproject.toml` today.

## 7. Module structure

Four small, independently testable components inside `eventlog_detector.py` (plus a markers file):

### 7.1 `_resolve_event_log_file(path) -> (source, resolved)`

Path resolver. Turns user input into the concrete file to read.

- File input → return as-is.
- Directory input → pattern match against rolling layouts:
  - **Spark native** (`eventlog_v2_*` directory): list children via `CspFs.list_all_files(dir_path)`; pick the lexicographically earliest file whose name matches `events_1_*` (chunk index 1, which carries `ApplicationStart` + `EnvironmentUpdate`).
  - **Databricks** (files named `eventlog` or `eventlog-<DATE>--<HH-MM>[.codec]`): list children via `CspFs.list_all_files(dir_path)`; sort using the same rule as Scala's `DatabricksRollingEventLogFilesFileReader` (`core/src/main/scala/com/nvidia/spark/rapids/tool/EventLogPathProcessor.scala:458-478, 496-500`) — parsed `LocalDateTime` from the filename, with bare `eventlog` treated as *latest* (i.e., sorted last); pick the earliest. That is the file carrying app-start.
- Anything else (generic multi-app dir, wildcard, comma list) → raise `UnsupportedEventLogShapeError`.
- Pattern matching only — no file reads for the shape decision.

### 7.2 `_open_event_log_stream(resolved_path) -> Iterator[str]`

Stream opener. Opens the file with the right codec and yields decoded text lines.

- Codec chosen by extension.
- Codec libs imported lazily inside this function — missing lib raises `UnsupportedCompressionError`, does not fail the module import.
- Cloud paths handled via `CspPath.open_input_stream()`; the returned stream is wrapped by the codec reader, then by a text decoder yielding lines.

### 7.3 `_scan_events(lines, max_events) -> _ScanResult`

Event scanner. Parses lines as JSON, merges properties from the events Scala uses for runtime classification, and stops when enough state is accumulated (or the cap is reached).

**Events the scanner must consume** (all of these affect runtime classification in Scala):

| Event | Scala handler | Effect on classification state |
| --- | --- | --- |
| `SparkListenerLogStart` | `EventProcessorBase.doSparkListenerLogStart` → `handleLogStartForCachedProps` | Captures Spark version |
| `SparkListenerApplicationStart` | populates `appMetaData` | Captures appId / appName |
| `SparkListenerEnvironmentUpdate` | `handleEnvUpdateForCachedProps` → `updatePredicatesFromSparkProperties` → `reEvaluate(sparkProperties)` + `gpuMode ||= isPluginEnabled(...)` | Seeds the main `Spark Properties` dict; initial plugin evaluation |
| `SparkListenerJobStart` | `handleJobStartForCachedProps` → `reEvaluateOnJobLevel(jobProperties)` | Re-evaluates plugins with `hasJobLevelConfigs=true` (DB, Iceberg, Hive). Only matters for Photon/DB here (Auron and SPARK_RAPIDS are driver-level and already settled) |
| `SparkListenerSQLExecutionStart` | `doSparkListenerSQLExecutionStart` → `mergeModifiedConfigs(modifiedConfigs)` → `updatePredicatesFromSparkProperties` | Merges per-SQL config overrides into `sparkProperties`; can turn on gpuMode or a plugin whose key was not present at env-update time |

Scanning only `SparkListenerEnvironmentUpdate` would diverge from Scala for logs that enable `spark.plugins` / `spark.rapids.sql.enabled` / Databricks tags via `modifiedConfigs` or job-level properties. We consume all five events above.

**Property merging (mirrors Scala):**

- `spark_properties` = env-update `Spark Properties` with per-SQL `modifiedConfigs` merged last-write-wins (matches `CacheablePropsHandler.mergeModifiedConfigs`).
- For DB plugin specifically (`hasJobLevelConfigs=true`): also feed `SparkListenerJobStart.properties` to the DB precondition check. All three DB tag keys must be non-empty from the combined property pool (env-update ∪ job-level ∪ modifiedConfigs).
- Classification after each merge follows the exact rules in section 8.

**Sticky semantics:** plugin matches are sticky (once true, stay true — `AppPropPlugTrait:66-68`). Same for gpuMode. So classification can only become more specific over time (SPARK → PHOTON/AURON/SPARK_RAPIDS), never less.

**Stop conditions:**

- Early-stop: as soon as `SparkListenerEnvironmentUpdate` has been seen AND the classification is non-SPARK, we can return. A non-SPARK label is sticky.
- Extended-scan: if classification is still SPARK after env-update, keep scanning `SparkListenerJobStart` and `SparkListenerSQLExecutionStart` events, merging their properties, re-classifying after each merge. Stop at first non-SPARK classification or at `max_events_scanned`.
- Cap: `max_events_scanned` default raised from the original 1000 to **2000** to accommodate SQLExecutionStart events that can land later in the log. `SparkListenerApplicationStart` is also captured if/when seen during the extended scan.

**Malformed input:** lines that aren't valid JSON are skipped (Spark tolerates trailing partial lines in live logs).

**Terminal states:**

- EOF or cap with env-update seen → returns `_ScanResult` with the final classification state. If classification is still SPARK here, that is the final answer (matches what Scala would emit given the same prefix).
- EOF or cap with env-update NOT seen → raises `EventLogIncompleteError`. Classification is unknown and the caller should fall back to the full pipeline.

### 7.4 `_classify_runtime(spark_properties) -> SparkRuntime`

Runtime classifier. Pure function over the properties dict. See section 8 for the exact rules.

### 7.5 `eventlog_detector_markers.py`

Single source of truth for keys/regex/substrings. Every constant has a `# Scala source: <file>:<line>` comment next to it.

## 8. Classification rules (synced with Scala)

Priority order evaluated in Python: **PHOTON > AURON > SPARK_RAPIDS > SPARK**. This is a deliberate, deterministic Python choice; Scala's `pluginMap.values.filter(...).find(...)` iterates a `HashMap` and is non-deterministic when multiple plugins match (they don't overlap in practice).

**SPARK_RAPIDS** — `ToolUtils.isPluginEnabled` (`core/src/main/scala/org/apache/spark/sql/rapids/tool/ToolUtils.scala:114-121`):

- `spark.plugins` contains substring `com.nvidia.spark.SQLPlugin`
- AND `spark.rapids.sql.enabled` parses as boolean true (default true if missing or unparseable)

**AURON** — `AuronParseHelper.eval` (`core/src/main/scala/com/nvidia/spark/rapids/tool/planparser/auron/AuronParseHelper.scala:149-172`):

- `spark.sql.extensions` fullmatches regex `.*AuronSparkSessionExtension.*`
- AND `spark.auron.enabled` trimmed equals `"true"` case-insensitively (default `"true"` if missing)

**PHOTON** — requires Databricks precondition + Photon marker:

- Databricks precondition (`DBConditionImpl.eval`, `core/src/main/scala/com/nvidia/spark/rapids/tool/planparser/db/DBPlugin.scala:45-58`): all three of `spark.databricks.clusterUsageTags.clusterAllTags`, `.clusterId`, `.clusterName` are non-empty.
- AND Photon marker (`PhotonParseHelper.extensionRegxMap`, `core/src/main/scala/com/nvidia/spark/rapids/tool/planparser/db/DatabricksParseHelper.scala:146-151`): any one of these fullmatches:
  - `spark.databricks.clusterUsageTags.sparkVersion` ~ `.*-photon-.*`
  - `spark.databricks.clusterUsageTags.effectiveSparkVersion` ~ `.*-photon-.*`
  - `spark.databricks.clusterUsageTags.sparkImageLabel` ~ `.*-photon-.*`
  - `spark.databricks.clusterUsageTags.runtimeEngine` ~ `PHOTON`

**SPARK** — none of the above.

### Notes on fidelity

- Scala's `String.matches(regex)` requires whole-string match → Python uses `re.fullmatch`.
- Plugin-based runtime beats gpuMode: if a log has both `com.nvidia.spark.SQLPlugin` and Photon markers, Scala returns `PHOTON`. Python priority preserves this.
- `spark.rapids.sql.enabled` default-to-true semantics match `ToolUtils.isPluginEnabled` exactly (`Try { ... }.getOrElse(true)`).

## 9. Error model

All errors subclass `EventLogDetectionError`:

| Exception | Meaning | aether action |
| --- | --- | --- |
| `UnsupportedEventLogShapeError` | Input is multi-app / wildcard / comma list | Fall back to running the full tool |
| `UnsupportedCompressionError` | Codec lib missing | Install the extra; surface message to dev |
| `EventLogReadError` | I/O failure (wraps underlying error) | Retry or fall back |
| `EventLogIncompleteError` | Env-update event not found in scanned range | Fall back — classification not known |

Typed exceptions (not `Optional[RuntimeInfo]`) so aether can distinguish "log is CPU" from "we couldn't tell."

## 10. Testing

### 10.1 Unit tests — `tests/spark_rapids_tools_ut/tools/test_eventlog_detector.py`

- Path resolver: plain file, Spark rolling dir (multi-chunk), Databricks rolling dir, multi-app dir raises, wildcard raises.
- Stream opener: each codec including missing-lib path raising.
- Event scanner: truncated log raises, malformed JSON lines skipped, `max_events_scanned` cap.
- Classifier: each of the four runtime outcomes, priority when multiple markers coexist, `spark.rapids.sql.enabled=false` override.

### 10.2 Parity test — `tests/spark_rapids_tools_ut/tools/test_eventlog_detector_parity.py`

Runs `detect_spark_runtime` against existing Scala fixtures under `core/src/test/resources/spark-events-*`. Expected labels come from existing Scala test expectations (e.g., `eventlog-gpu-dsv2.zstd` → `SPARK_RAPIDS`). Fails CI if Python disagrees.

### 10.3 Follow-up (not V1)

A Scala-side test that synthesizes property maps, exercises each plugin, and exports a JSON fixture of `(properties, expected_runtime)` pairs. The Python parity test loads this JSON and replays classification. Catches Scala-side marker changes automatically. Deferred to a separate PR after V1 lands.

## 11. Rollout

- Single PR, `[FEA]` tag, references issue #2082.
- Additive only — no breaking changes.
- aether integration lands as a separate PR in `aether-services` after this merges.

## 12. Open items for implementation plan

- Exact location / filename of the short user-facing doc (match whatever existing convention `user_tools/docs/` uses).
- Whether the `zstandard` dep addition warrants an entry in `RELEASE.md` or similar.
- Final pick for `.lzf` codec package (`python-lzf` vs alternative); the plan should verify it installs cleanly alongside current `user_tools/pyproject.toml` constraints before committing.
- Whether we keep `.snappy` / `.lzf` behind the `[compression]` extra (current plan) or fold them in as hard deps (simpler but bigger install).
- Parity-test fixture inventory: enumerate every file under `core/src/test/resources/spark-events-*` and record the expected `SparkRuntime` label, derived from existing Scala test expectations. The plan step owns this list.

## 13. Review feedback addressed (2026-04-22)

Findings applied against the initial draft of this spec:

1. **Scan scope extended beyond env-update** (section 7.3). The scanner now also consumes `SparkListenerJobStart` and `SparkListenerSQLExecutionStart` and merges their property sets into classification, mirroring `EventProcessorBase.doSparkListenerSQLExecutionStart` / `handleJobStartForCachedProps` and `CacheablePropsHandler.mergeModifiedConfigs`. This closes the gap where a log enables `spark.plugins` via `modifiedConfigs` or Databricks tags via job-level properties.
2. **Databricks rolling-dir file selection** (sections 6 and 7.1). File pick is now an explicit mirror of `DatabricksRollingEventLogFilesFileReader`: parse `--YYYY-MM-DD--HH-MM` from each filename, treat bare `eventlog` as latest (sort last), pick the earliest. That is the file that carries app-start.
3. **Storage API corrected** (section 7.1, 7.2). Uses `CspFs.list_all_files(path)` and `CspPath.open_input_stream()`. `BoundedCspPath.list_dir()` / `.open()` do not exist.
4. **Compression set and packaging fixed** (section 6). `.lzf` is added alongside `.lz4`/`.snappy`/`.zstd` to match Scala's `SPARK_SHORT_COMPRESSION_CODEC_NAMES` and the existing `user_tools/docs/user-tools-onprem.md` claim. Install guidance uses the correct distribution name `spark-rapids-user-tools` and the `[compression]` extra is explicitly called out as a new extra introduced by this feature.