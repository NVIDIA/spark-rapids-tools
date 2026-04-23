# Lightweight Event Log Runtime Detector — Design

- **Issue:** [NVIDIA/spark-rapids-tools#2082](https://github.com/NVIDIA/spark-rapids-tools/issues/2082)
- **Branch:** `sbari-issue-2082`
- **Date:** 2026-04-22
- **Status:** Design approved, ready for implementation plan

## 1. Problem

Determining whether a Spark event log came from a CPU or GPU run currently requires running the full qualification or profiling tool. That is expensive, and it is the wrong way to make the pre-flight decision of *which* tool to run.

aether (a consumer of spark-rapids-tools) needs that decision per job. Its current workaround is running one tool, reading `application_information.csv`'s `sparkRuntime`, and picking the right tool next — meaning every routing decision costs a full qualification run.

## 2. Goal

Provide a lightweight Python function that reads a bounded prefix of a Spark event log and returns a **routing decision** (`QUALIFICATION`, `PROFILING`, or `UNKNOWN`) plus best-effort metadata (`spark_runtime`, `app_id`, `spark_version`).

**This is best-effort early routing, not exact Scala parity.** On inconclusive input, the caller falls back to the full tool.

## 3. Non-goals (V1)

- Replacing the Scala event log reader. Multi-app directories, wildcards, comma lists, malformed logs, the full codec set, and most CSP quirks stay in Scala.
- Exact classification parity with Scala. Scala can adjust runtime state from events far into the log (job-level plugin re-evaluation, SQL-level `modifiedConfigs`, per-SQL merges). The Python detector only looks at a bounded prefix; the caller must be comfortable with `UNKNOWN` for genuinely ambiguous inputs.
- Public user-facing CLI. Dev/library scope only.
- Writing any output files. Result is a Python object returned in-process.

## 4. Consumers

- **Primary:** aether (`aether-services`). aether already consumes `spark_rapids_tools` as a library. It will import `detect_spark_runtime` and branch on the returned `Route` before deciding to run qualification or profiling. On `Route.UNKNOWN` or any raised error, aether falls back to running qualification (its current default).
- **Secondary:** internal scripting / debugging. Devs can `python -c "..."` to inspect a log.

## 5. Public API

Module: `user_tools/src/spark_rapids_tools/tools/eventlog_detector.py`.

```python
from enum import Enum
from dataclasses import dataclass
from typing import Optional, Union
from spark_rapids_tools.storagelib.csppath import CspPath

class Route(str, Enum):
    QUALIFICATION = "QUALIFICATION"  # CPU log → run qualification
    PROFILING = "PROFILING"          # GPU/Photon/Auron log → run profiling
    UNKNOWN = "UNKNOWN"              # not enough signal; caller falls back

class SparkRuntime(str, Enum):
    SPARK = "SPARK"
    SPARK_RAPIDS = "SPARK_RAPIDS"
    PHOTON = "PHOTON"
    AURON = "AURON"

@dataclass(frozen=True)
class DetectionResult:
    route: Route
    spark_runtime: Optional[SparkRuntime]   # best-effort; may be None when route is UNKNOWN
    app_id: Optional[str]
    spark_version: Optional[str]
    event_log_path: str                     # concrete file actually read
    source_path: str                        # caller input (preserves rolling-dir input)
    reason: str                             # short human-readable explanation

def detect_spark_runtime(
    event_log: Union[str, CspPath],
    *,
    max_events_scanned: int = 500,
) -> DetectionResult: ...
```

`SparkRuntime` string values match the Scala enum so aether's existing `JobRun.spark_runtime` column stays compatible.

## 6. Scope of input shapes (V1)

| Input shape | Supported | Notes |
| --- | --- | --- |
| Single event log file (plain or compressed) | ✅ | Primary case |
| Databricks rolling dir (`eventlog` + optional `eventlog-<date>` files) | ✅ | Required by aether — see 7.1 for the selection rule |
| Spark native rolling dir (`eventlog_v2_*`) | ❌ | Out of scope for V1. Raises `UnsupportedInputError`. Caller falls back. |
| Generic directory of independent logs | ❌ | Same |
| Wildcard path | ❌ | Same |
| Comma-separated list | ❌ | Same |

**Supported compression codecs:** plain, `.gz`, `.zstd`/`.zst`. Anything else (`.lz4`, `.lzf`, `.snappy`, unknown) raises `UnsupportedCompressionError` → caller falls back.

`zstandard` becomes a required dependency (existing test fixtures use `.zstd`; the common Spark default). No new optional extras.

## 7. Module structure

Four small, independently testable components inside `eventlog_detector.py` (plus a markers file).

### 7.1 `_resolve_event_log_file(path) -> (source, resolved)`

Path resolver. Turns user input into the concrete file to read.

- File input → return as-is.
- Directory input → Databricks-rolling shape only:
  - Use `CspFs.list_all_files(dir_path)` to list children.
  - Recognize Databricks files by the same prefix Scala uses in `EventLogPathProcessor.isDBEventLogFile` (`DB_EVENT_LOG_FILE_NAME_PREFIX = "eventlog"`).
  - Sort them exactly like Scala's `DatabricksRollingEventLogFilesFileReader` (`core/src/main/scala/com/nvidia/spark/rapids/tool/EventLogPathProcessor.scala:458-478, 496-500`): parse `LocalDateTime` from the `eventlog-YYYY-MM-DD--HH-MM[.codec]` pattern; bare `eventlog` (no `--date`) is treated as the latest and sorted last.
  - Pick the earliest file. `SparkListenerApplicationStart` and `SparkListenerEnvironmentUpdate` live there.
  - If the directory contains no Databricks-pattern files → raise `UnsupportedInputError`.
- Anything else (Spark-native rolling, generic multi-app dir, wildcard, comma list) → raise `UnsupportedInputError`.
- Pattern matching only — no file reads for the shape decision.

### 7.2 `_open_event_log_stream(resolved_path) -> Iterator[str]`

Stream opener. Opens the file with the right codec and yields decoded text lines.

- Codec chosen by extension: plain / `.inprogress` / `.gz` / `.zstd` / `.zst`. Anything else → `UnsupportedCompressionError`.
- Cloud paths use `CspPath.open_input_stream()`; the byte stream is wrapped by the codec reader, then by a text decoder yielding lines.

### 7.3 `_scan_events(lines, max_events) -> _ScanResult`

Event scanner. Parses lines as JSON and accumulates classification-relevant properties.

**Events consumed:**

| Event | What it contributes |
| --- | --- |
| `SparkListenerLogStart` | Spark version |
| `SparkListenerApplicationStart` | appId, appName |
| `SparkListenerEnvironmentUpdate` | Seeds `spark_properties` from its `Spark Properties` section |
| `SparkListenerSQLExecutionStart` | Merges `modifiedConfigs` into `spark_properties` (last-write-wins) — covers logs where `spark.plugins` / `spark.rapids.sql.enabled` are set per-SQL rather than at startup |

We intentionally do not track `SparkListenerJobStart` job-level properties. That would catch the DB plugin's job-level re-evaluation path, but that is a narrow Scala case and pushing further into the log moves us away from "lightweight." If a log is truly Databricks-only-detectable at job-start time, the caller's fallback path handles it.

**Stop conditions:**

- Early-stop: as soon as `_classify_runtime(spark_properties)` returns anything other than `SPARK` (the signal is decisive and Scala's plugins are sticky-true — once set, they stay set).
- EOF or `max_events_scanned` reached: return whatever was accumulated.
- `max_events_scanned` default `500`. Startup events land in the first ~20; the rest is headroom for the first few `SQLExecutionStart` merges.

**Malformed input:** lines that aren't valid JSON are skipped.

**Returned state:** `(spark_properties, app_id, app_name, spark_version, env_update_seen)`.

### 7.4 `_classify_runtime(spark_properties) -> SparkRuntime`

Pure function over the accumulated properties dict. See section 8 for rules.

### 7.5 Top-level flow

```
detect_spark_runtime(path):
    source, resolved = _resolve_event_log_file(path)
    with _open_event_log_stream(resolved) as lines:
        scan = _scan_events(lines, max_events_scanned)

    if not scan.env_update_seen:
        return DetectionResult(route=UNKNOWN, spark_runtime=None, ...,
                               reason="no SparkListenerEnvironmentUpdate before cap")

    runtime = _classify_runtime(scan.spark_properties)
    route = PROFILING if runtime in {SPARK_RAPIDS, PHOTON, AURON} else QUALIFICATION
    return DetectionResult(route=route, spark_runtime=runtime, ...,
                           reason=f"classified as {runtime.value}")
```

### 7.6 `eventlog_detector_markers.py`

Single source of truth for keys/regex/substrings. Every constant carries a `# Scala source: <file>:<line>` comment.

## 8. Classification rules

Mapped from the same Scala sources. Priority order in Python: **PHOTON > AURON > SPARK_RAPIDS > SPARK**.

**SPARK_RAPIDS** — from `ToolUtils.isPluginEnabled` (`core/src/main/scala/org/apache/spark/sql/rapids/tool/ToolUtils.scala:114-121`):

- `spark.plugins` contains substring `com.nvidia.spark.SQLPlugin`
- AND `spark.rapids.sql.enabled` parses as boolean true (default true if missing/unparseable)

**AURON** — from `AuronParseHelper.eval` (`core/src/main/scala/com/nvidia/spark/rapids/tool/planparser/auron/AuronParseHelper.scala:149-172`):

- `spark.sql.extensions` fullmatches `.*AuronSparkSessionExtension.*`
- AND `spark.auron.enabled` trimmed equals `"true"` case-insensitively (default `"true"`)

**PHOTON** — combined precondition + marker:

- Databricks precondition (`DBConditionImpl.eval`, `core/src/main/scala/com/nvidia/spark/rapids/tool/planparser/db/DBPlugin.scala:45-58`): all three of `spark.databricks.clusterUsageTags.clusterAllTags`, `.clusterId`, `.clusterName` are non-empty.
- AND any Photon marker (`PhotonParseHelper`, `core/src/main/scala/com/nvidia/spark/rapids/tool/planparser/db/DatabricksParseHelper.scala:146-151`) fullmatches:
  - `spark.databricks.clusterUsageTags.sparkVersion` ~ `.*-photon-.*`
  - `spark.databricks.clusterUsageTags.effectiveSparkVersion` ~ `.*-photon-.*`
  - `spark.databricks.clusterUsageTags.sparkImageLabel` ~ `.*-photon-.*`
  - `spark.databricks.clusterUsageTags.runtimeEngine` ~ `PHOTON`

**SPARK** — none of the above matched on the accumulated properties.

Python uses `re.fullmatch` (matches Scala's `String.matches` semantics).

## 9. Error model

All errors subclass `EventLogDetectionError`:

| Exception | Meaning | aether action |
| --- | --- | --- |
| `UnsupportedInputError` | Input shape not supported (Spark-native rolling, multi-app dir, wildcard, comma list, empty dir) | Fall back to running the full tool |
| `UnsupportedCompressionError` | Codec outside the supported set | Fall back |
| `EventLogReadError` | I/O failure (wraps underlying error) | Fall back |

**Note:** "scanner never saw env-update" is **not** an exception — it's a `DetectionResult` with `route=UNKNOWN`. This keeps the caller's happy path free of exception handling for the common "inconclusive log" case.

## 10. Testing

### 10.1 Unit tests — `tests/spark_rapids_tools_ut/tools/test_eventlog_detector.py`

- Path resolver: plain file; Databricks rolling dir (multi-file with dated + bare `eventlog`, asserting earliest picked); Spark-native rolling dir raises; multi-app dir raises; wildcard raises.
- Stream opener: plain / gz / zstd each works; `.lz4`/`.snappy` raises.
- Event scanner: env-update only → classifies from it; env-update + later SQLExecutionStart that sets `spark.plugins` → classification updates to `SPARK_RAPIDS`; no env-update within cap → `UNKNOWN`; malformed JSON lines skipped.
- Classifier: each of the four runtime outcomes, priority when multiple markers coexist, `spark.rapids.sql.enabled=false` override.
- Routing: runtime → route mapping.

### 10.2 Fixture tests — `tests/spark_rapids_tools_ut/tools/test_eventlog_detector_fixtures.py`

Runs `detect_spark_runtime` against a small curated set of existing fixtures under `core/src/test/resources/spark-events-*`:

- `eventlog-gpu-dsv2.zstd` → `Route.PROFILING`, `SPARK_RAPIDS`
- `eventlog_dsv2.zstd` → `Route.QUALIFICATION`, `SPARK`
- A Databricks rolling fixture (to be identified during plan; if none exists we'll synthesize one)

Not a full parity sweep — just anchor points to catch regressions.

## 11. Rollout

- Single PR, `[FEA]` tag, references issue #2082.
- Additive only — no breaking changes.
- aether integration lands as a separate PR in `aether-services` after this merges.

## 12. Open items for implementation plan

- Confirm `core/src/test/resources/` contains a usable Databricks-rolling fixture; synthesize one if not.
- Whether adding `zstandard` as a hard dep needs a `RELEASE.md` note.

## 13. Evolution

This spec was reshaped once after review feedback. Earlier drafts attempted:

- Full scan-scope parity with Scala (including `SparkListenerJobStart.properties` job-level plugin re-evaluation) — dropped. Documented as a known divergence case; caller handles it via fallback on `Route.UNKNOWN`.
- Spark-native rolling-dir support — dropped from V1. Aether's primary input is single files or Databricks rolling dirs.
- `.lz4` / `.lzf` / `.snappy` codec support with a new `[compression]` extra — dropped. Added scope without matching a real need.
- 4-way `SparkRuntime` return as the primary contract — kept as auxiliary metadata; primary contract is now the `Route` enum (the actual decision the caller makes).

The narrower V1 keeps the detector honest about what it is: a best-effort fast path that gets out of the way when the log doesn't give it enough signal.