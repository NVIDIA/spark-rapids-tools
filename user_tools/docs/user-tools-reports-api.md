# User Tools Reports API Documentation

This documentation provides a comprehensive guide for using the Spark Rapids Tools API v1 to load and analyze qualification and profiling tool outputs.

## Table of Contents

1. [Overview](#overview)
2. [Creating Result Handlers](#creating-result-handlers)
3. [Reading Report Data](#reading-report-data)
4. [Working with Applications](#working-with-applications)
5. [Return Types and Data Handling](#return-types-and-data-handling)
6. [Error Handling](#error-handling)
7. [Complete Examples](#complete-examples)
8. [Best Practices](#best-practices)

## Overview

The Spark Rapids Tools API v1 provides a simple interface for:

- **Loading tool outputs** from qualification and profiling runs
- **Reading reports** in multiple formats (CSV, JSON Properties, TXT)
- **Accessing per-application data** with type-safe operations
- **Supporting cloud and local storage** paths seamlessly

### Supported Report Types

| Report Type | Description | Return Type |
|-------------|-------------|-------------|
| CSV | Tabular data with pandas DataFrames | `LoadDFResult` |
| JSON Properties | Configuration and metadata | `JPropsResult` |
| TXT | Plain text logs and outputs | `TXTResult` |

## Creating Result Handlers

### Basic Handler Creation

Create handlers using specialized classes for each tool output type:

```python
from spark_rapids_tools.api_v1 import QualCore, ProfCore, QualWrapper, ProfWrapper

# Qualification Core Output (qual_core_output/)
qual_handler = QualCore("/path/to/qual_core_output")

# Profiling Core Output (rapids_4_spark_profile/)
prof_handler = ProfCore("/path/to/rapids_4_spark_profile")

# Qualification Wrapper Output (qual_<timestamp>/)
qual_wrap_handler = QualWrapper("/path/to/qual_20250101000000_12345678")

# Profiling Wrapper Output
prof_wrap_handler = ProfWrapper("/path/to/prof_output")
```

### Dynamic Handler Creation

For dynamic report ID resolution:

```python
from spark_rapids_tools.api_v1 import APIResHandler

# Create handler by report ID
handler = APIResHandler.from_id(
    report_id="qualCoreOutput",
    out_path="/path/to/qual_core_output"
)
```

### Cloud Storage Paths

The API supports cloud storage seamlessly:

```python
from spark_rapids_tools.api_v1 import QualCore

# S3
handler = QualCore("s3://my-bucket/qual_core_output")

# GCS
handler = QualCore("gs://my-bucket/qual_core_output")


# HDFS
handler = QualCore("hdfs://namenode:port/path/to/qual_core_output")
```

## Reading Report Data

### CSV Reports

CSV reports provide tabular data wrapped in LoadDFResult objects containing pandas DataFrames.

#### Global Table Data

```python
from spark_rapids_tools.api_v1 import QualCore

# Create handler
handler = QualCore("/path/to/qual_core_output")

# Load global summary data using convenience method
summary_result = handler.csv("qualCoreCSVSummary").load()

# Access the DataFrame
df = summary_result.data
print(f"Loaded {len(df)} rows of qualification data")
```

#### Per-Application Table Data

```python
# Load data for a single application
app_result = handler.csv("execCSVReport").app("application_001").load()
df = app_result.data

# Load data for multiple applications at once
multi_app_results = handler.csv("execCSVReport").apps(["app_001", "app_002"]).load()

# Results is a dictionary mapping app_id -> LoadDFResult
for app_id, result in multi_app_results.items():
    if result.success:
        print(f"{app_id}: {len(result.data)} rows")
```

#### Advanced CSV Options

```python
from spark_rapids_tools.api_v1 import QualCore

handler = QualCore("/path/to/qual_core_output")

# Custom pandas read arguments
custom_result = handler.csv("execCSVReport").pd_args({
    "sep": ",",
    "encoding": "utf-8",
    "index_col": 0
}).app("application_001").load()

# Column mapping for renaming
mapped_result = handler.csv("stagesCSVReport").map_cols({
    "old_column_name": "new_column_name"
}).app("application_001").load()

# Fallback data when table is missing
import pandas as pd

def create_fallback_data():
    return pd.DataFrame({"app_id": ["fallback_app"], "status": ["no_data"]})

result_with_fallback = (handler.csv("optional_table")
                        .fall_cb(create_fallback_data)
                        .app("application_001")
                        .load())
```

### JSON Properties Reports

Properties files (runtime.properties, etc.) return key-value pairs:

```python
from spark_rapids_tools.api_v1 import QualCore

handler = QualCore("/path/to/qual_core_output")

# Load global properties
global_props = handler.jprop("runtimeProperties").load()

# Access property data
print(f"Tool version: {global_props.props.get('version')}")
print(f"All properties: {global_props.props}")
```

### TXT Reports

Text reports handle plain text data like logs:

```python
from spark_rapids_tools.api_v1 import QualCore

handler = QualCore("/path/to/qual_core_output")

# Load text report for a single application  
app_text = handler.txt("tuningRecommendationsLog").app("application_001").load()

# Access text content
if app_text.success:
    print(app_text.data)

# Load for multiple applications
app_texts = handler.txt("tuningRecommendationsLog").apps(["app_001", "app_002"]).load()

for app_id, result in app_texts.items():
    if result.success:
        print(f"--- {app_id} ---")
        print(result.data)
```

## Working with Applications

### Application Identifiers

The API supports both string identifiers and AppHandler objects:

```python
from spark_rapids_tools.api_v1 import AppHandler

# Using string identifiers
result1 = (CSVReport(handler)
    .table("per_app_table")
    .app("application_1234567890_0001")
    .load())

# Using AppHandler objects
app_handler = AppHandler("application_1234567890_0001")
result2 = (CSVReport(handler)
    .table("per_app_table")
    .app(app_handler)
    .load())

# Multiple applications with mixed types
results = (CSVReport(handler)
    .table("per_app_table")
    .app("app_001")
    .app(AppHandler("app_002"))
    .apps(["app_003", "app_004"])
    .load())
```

### Application Selection Patterns

```python
# Select all applications from a specific run
all_apps = ["application_001", "application_002", "application_003"]
all_results = (CSVReport(handler)
    .table("sql_to_stage_exec_info")
    .apps(all_apps)
    .load())

# Filter applications by pattern (example with filtering)
filtered_apps = [app for app in all_apps if app.endswith("_001")]
filtered_results = (CSVReport(handler)
    .table("sql_to_stage_exec_info")
    .apps(filtered_apps)
    .load())
```

## Return Types and Data Handling

### Global Tables

Global tables return single result objects:

```python
# CSV: Returns LoadDFResult
csv_result = CSVReport(handler).table("global_summary").load()
dataframe = csv_result.data

# JSON Props: Returns JPropsResult
props_result = JPropsReport(handler).table("global_config").load()
properties = props_result.props

# TXT: Returns TXTResult
txt_result = TXTReport(handler).table("global_log").load()
text_content = txt_result.text
```

### Per-Application Tables

Per-application tables have different return types based on the number of applications:

```python
# Multiple applications: Returns Dict[str, ResultType]
multi_results = (CSVReport(handler)
    .table("per_app_table")
    .apps(["app_001", "app_002"])
    .load())

# Access individual results
app1_data = multi_results["app_001"].data
app2_data = multi_results["app_002"].data

# Single application: Returns ResultType directly (not a dictionary)
single_result = (CSVReport(handler)
    .table("per_app_table")
    .app("app_001")
    .load())

# Direct access to data
single_app_data = single_result.data
```

## Error Handling

### Input Validation

The API provides comprehensive input validation:

```python
try:
    # Empty report ID validation
    handler = APIResultHandler().report("").build()
except ValueError as e:
    print(f"Validation Error: {e}")

try:
    # Missing required path
    handler = APIResultHandler().qual_core().build()
except ValueError as e:
    print(f"Missing Path Error: {e}")

try:
    # Invalid application configuration
    result = (CSVReport(handler)
        .table("global_table")
        .app("app_001")  # Cannot specify apps for global tables
        .load())
except ValueError as e:
    print(f"Configuration Error: {e}")
```

### Data Loading Errors


If the data fails to be loaded or consumed, the error will be captured by the return object
and the success flag is set to false.

```python
try:
    result = (CSVReport(handler)
        .table("non_existent_table")
        .load())
    if not result.success:
        print(result.get_fail_cause())
except ValueError as e:
    print(f"Configuration Error: {e}")
```


## Complete Examples

### Qualification Analysis Workflow

```python
from spark_rapids_tools.api_v1 import QualCore

# Create qualification result handler
handler = QualCore("/data/qual_core_output")

# Load qualification summary
summary = handler.csv("qualCoreCSVSummary").load()

print(f"Analyzed {len(summary.data)} applications")

# Load detailed exec analysis for specific apps
target_apps = ["application_001", "application_002"]
exec_analysis = handler.csv("execCSVReport").apps(target_apps).load()

# Process results for each application
for app_id, result in exec_analysis.items():
    if result.success:
        df = result.data
        print(f"App {app_id}: {len(df)} executors analyzed")

        # Find unsupported operations
        unsupported = df[df['Exec Is Supported'] == False]
        if not unsupported.empty:
            print(f"  - {len(unsupported)} unsupported executors found")

# Load runtime properties
props = handler.jprop("runtimeProperties").load()

if props.success:
    print(f"Tool version: {props.props.get('toolsJarVersion', 'unknown')}")
```

### Profiling Analysis Workflow

```python
from spark_rapids_tools.api_v1 import ProfCore

# Create profiling result handler  
handler = ProfCore("/data/rapids_4_spark_profile/<driver-host>")

# Load application information
app_info = handler.csv("coreRawApplicationInformationCSV").load()

print(f"Profiled {len(app_info.data)} applications")

# Load executor information
executor_info = handler.csv("coreRawExecutorInformationCSV").load()

if executor_info.success:
    print(f"Executor cores: {executor_info.data['executorCores'].iloc[0]}")
    print(f"Executor memory: {executor_info.data['executorMemory'].iloc[0]}")
```

### Tuning Reports

Tuning reports provide AutoTuner recommendations for GPU migration (requires `--auto-tuner` flag).
All tuning files are in Spark command-line format (`--conf key=value`):

```python
from spark_rapids_tools.api_v1 import QualCore

# Create qualification handler
handler = QualCore("/path/to/qual_core_output")

# Load bootstrap configuration (required GPU configs)
bootstrap_conf = handler.txt("tuningBootstrapConf").app("application_001").load()

if bootstrap_conf.success:
    print("Bootstrap configurations (Spark CLI format):")
    print(bootstrap_conf.data)
    # Output format:
    # --conf spark.rapids.sql.enabled=true
    # --conf spark.executor.cores=16
    # --conf spark.executor.memory=32g
    # ...

# Load detailed recommendations with comments
recommendations = handler.txt("tuningRecommendationsLog").app("application_001").load()

if recommendations.success:
    print("\nDetailed Recommendations:")
    print(recommendations.data)
    # Output includes:
    # Spark Properties:
    #   --conf ...
    # Comments:
    #   - Explanation of each setting...

# Load combined configuration (optional - only if generated)
combined_conf = handler.txt("tuningCombinedConf").app("application_001").load()

if combined_conf.success:
    print("\nCombined configurations:")
    print(combined_conf.data)
else:
    print("Combined config not available (optional file)")

# Load tuning for multiple applications
app_ids = ["application_001", "application_002", "application_003"]
bootstrap_configs = handler.txt("tuningBootstrapConf").apps(app_ids).load()

for app_id, config in bootstrap_configs.items():
    if config.success:
        # Count lines starting with --conf
        num_configs = len([l for l in config.data.split('\n') if l.startswith('--conf')])
        print(f"{app_id}: {num_configs} configurations")

# Parse configurations into dictionary
def parse_spark_conf(text):
    """Parse Spark CLI format into dict."""
    configs = {}
    for line in text.strip().split('\n'):
        if line.startswith('--conf '):
            conf = line[7:]  # Remove '--conf '
            if '=' in conf:
                key, value = conf.split('=', 1)
                configs[key] = value
    return configs

bootstrap = handler.txt("tuningBootstrapConf").app("application_001").load()
if bootstrap.success:
    configs_dict = parse_spark_conf(bootstrap.data)
    print(f"Parsed {len(configs_dict)} configurations:")
    for key, value in configs_dict.items():
        print(f"  {key} = {value}")
```

### Multi-Format Data Integration

```python
def analyze_application_performance(output_path, app_id):
    """Complete analysis combining multiple report formats."""

    # Create handlers for both qualification and profiling
    qual_handler = (APIResultHandler()
        .qual_core()
        .with_path(f"{output_path}/qualification")
        .build())

    prof_handler = (APIResultHandler()
        .prof_core()
        .with_path(f"{output_path}/profiling")
        .build())

    results = {}

    # 1. Get qualification score
    try:
        qual_summary = (CSVReport(qual_handler)
            .table("rapids_4_spark_qualification_output")
            .load())

        app_qual = qual_summary.data[
            qual_summary.data['App ID'] == app_id
        ]

        if not app_qual.empty:
            results['qualification_score'] = app_qual.iloc[0]['Estimated GPU Speedup']
    except Exception as e:
        print(f"Could not load qualification data: {e}")

    # 2. Get detailed SQL operations
    try:
        sql_ops = (CSVReport(qual_handler)
            .table("sql_to_stage_exec_info")
            .app(app_id)
            .load())

        results['total_sql_ops'] = len(sql_ops.data)
        results['supported_ops'] = len(
            sql_ops.data[sql_ops.data['Exec Is Supported'] == True]
        )
    except Exception as e:
        print(f"Could not load SQL operations: {e}")

    # 3. Get profiling metrics
    try:
        app_metrics = (CSVReport(prof_handler)
            .table("application_information")
            .load())

        app_data = app_metrics.data[app_metrics.data['App ID'] == app_id]
        if not app_data.empty:
            results['duration_ms'] = app_data.iloc[0]['App Duration']
            results['executor_cores'] = app_data.iloc[0]['Executor Cores']
    except Exception as e:
        print(f"Could not load application metrics: {e}")

    # 4. Get configuration
    try:
        config = (JPropsReport(prof_handler)
            .table("application_configuration")
            .app(app_id)
            .load())

        results['spark_config'] = config.props
    except Exception as e:
        print(f"Could not load configuration: {e}")

    return results

# Usage
app_analysis = analyze_application_performance(
    "/data/spark-rapids-analysis",
    "application_1234567890_0001"
)

print(f"Analysis Results: {app_analysis}")
```

## Best Practices

### 1-Input Validation and Error Handling

```python
def create_robust_handler(report_type, output_path):
    """Create a result handler with proper validation."""
    if not output_path:
        raise ValueError("Output path cannot be empty")

    try:
        handler_builder = APIResultHandler()

        if report_type == "qualification":
            handler_builder = handler_builder.qual_core()
        elif report_type == "profiling":
            handler_builder = handler_builder.prof_core()
        elif report_type == "wrapper":
            handler_builder = handler_builder.qual_wrapper()
        else:
            raise ValueError(f"Unknown report type: {report_type}")

        return handler_builder.with_path(output_path).build()

    except Exception as e:
        print(f"Failed to create handler: {e}")
        raise
```

### 2-Efficient Data Loading

```python
# Use fallback callbacks for optional tables
def create_empty_dataframe():
    import pandas as pd
    return pd.DataFrame(columns=['App ID', 'Status'])

optional_data = (CSVReport(handler)
    .table("optional_metrics")
    .fall_cb(create_empty_dataframe)
    .load())

# Optimize pandas reading for large files
large_data = (CSVReport(handler)
    .table("large_dataset")
    .pd_args({
        "chunksize": 10000,      # Read in chunks
        "low_memory": False,     # Better type inference
        "dtype": {"App ID": str} # Explicit types
    })
    .load())
```

### 3-Application Management

```python
def get_application_list(handler):
    """Get list of available applications from summary table."""
    try:
        summary = (CSVReport(handler)
            .table("rapids_4_spark_qualification_output")
            .load())
        return summary.data['App ID'].unique().tolist()
    except Exception:
        return []

def batch_process_applications(handler, table_name, app_list, batch_size=5):
    """Process applications in batches to manage memory."""
    results = {}

    for i in range(0, len(app_list), batch_size):
        batch = app_list[i:i + batch_size]

        try:
            batch_results = (CSVReport(handler)
                .table(table_name)
                .apps(batch)
                .load())

            results.update(batch_results)
        except Exception as e:
            print(f"Failed to process batch {i//batch_size + 1}: {e}")

    return results
```

### 4-Type Safety and Documentation

```python
from typing import Dict, List, Union, Optional
from spark_rapids_tools.api_v1.builder import LoadDFResult

def load_application_data(
    handler,
    table_name: str,
    apps: Union[str, List[str]],
    required: bool = True
) -> Optional[Union[LoadDFResult, Dict[str, LoadDFResult]]]:
    """
    Load CSV data for specified applications.

    Args:
        handler: Result handler instance
        table_name: Name of the table to load
        apps: Single app ID or list of app IDs
        required: Whether the table is required (affects error handling)

    Returns:
        LoadDFResult for single app, Dict[str, LoadDFResult] for multiple apps,
        or None if table not found and not required
    """
    try:
        report = CSVReport(handler).table(table_name)

        if isinstance(apps, str):
            return report.app(apps).load()
        else:
            return report.apps(apps).load()

    except FileNotFoundError:
        if required:
            raise
        return None
    except Exception as e:
        print(f"Error loading {table_name}: {e}")
        if required:
            raise
        return None
```


### 5-Resource Management

```python
# Use context managers for large datasets
class ReportAnalysis:
    def __init__(self, handler):
        self.handler = handler
        self._cached_data = {}

    def get_table_data(self, table_name, apps=None, use_cache=True):
        """Get table data with optional caching."""
        cache_key = f"{table_name}_{apps}"

        if use_cache and cache_key in self._cached_data:
            return self._cached_data[cache_key]

        report = CSVReport(self.handler).table(table_name)

        if apps:
            if isinstance(apps, str):
                result = report.app(apps).load()
            else:
                result = report.apps(apps).load()
        else:
            result = report.load()

        if use_cache:
            self._cached_data[cache_key] = result

        return result

    def clear_cache(self):
        """Clear cached data to free memory."""
        self._cached_data.clear()
```
