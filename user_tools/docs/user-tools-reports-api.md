# User Tools Reports API Documentation

This documentation provides a comprehensive guide for using the Spark Rapids Tools API v1 to create result handlers and read various report formats.

## Table of Contents

1. [Overview](#overview)
2. [Building Result Handlers](#building-result-handlers)
3. [Reading Report Data](#reading-report-data)
4. [Working with Applications](#working-with-applications)
5. [Return Types and Data Handling](#return-types-and-data-handling)
6. [Error Handling](#error-handling)
7. [Complete Examples](#complete-examples)
8. [Best Practices](#best-practices)

## Overview

The Spark Rapids Tools API v1 provides a fluent builder interface for:

- **Creating result handlers** for different tool outputs (qualification, profiling)
- **Loading data** from reports in multiple formats (CSV, JSON Properties, TXT)
- **Handling both global and per-application data** with type-safe operations
- **Managing cloud and local storage paths** seamlessly

### Supported Report Types

| Report Type | Description | Return Type |
|-------------|-------------|-------------|
| CSV | Tabular data with pandas DataFrames | `LoadDFResult` |
| JSON Properties | Configuration and metadata | `JPropsResult` |
| TXT | Plain text logs and outputs | `TXTResult` |

## Building Result Handlers

### Basic Result Handler Creation

The `APIResultHandler` class provides methods to build handlers for different tool outputs:

```python
from spark_rapids_tools.api_v1.builder import APIResultHandler

# Qualification Core Output
qual_handler = (APIResultHandler()
    .qual_core()
    .with_path("/path/to/qualification/output")
    .build())

# Profiling Core Output
prof_handler = (APIResultHandler()
    .prof_core()
    .with_path("/path/to/profiling/output")
    .build())
```

### Custom Report Handlers

For custom or specialized reports:

```python
from spark_rapids_tools.api_v1.builder import APIResultHandler
# Custom report with specific ID
custom_handler = (APIResultHandler()
    .report("customReportId")
    .with_path("/path/to/custom/output")
    .build())
```

### Path Configuration

The API supports both local and cloud storage paths:

```python
from spark_rapids_tools.storagelib.cspfs import BoundedCspPath

# Local filesystem path
local_handler = (APIResultHandler()
    .qual_core()
    .with_path("/local/path/to/output")
    .build())

# Cloud storage path (S3, GCS, Azure)
cloud_path = BoundedCspPath("s3://my-bucket/spark-rapids/output")
cloud_handler = (APIResultHandler()
    .qual_core()
    .with_path(cloud_path)
    .build())

# HDFS path
hdfs_handler = (APIResultHandler()
    .qual_core()
    .with_path("hdfs://namenode:port/path/to/output")
    .build())
```

## Reading Report Data

### CSV Reports

CSV reports provide tabular data wrapped in LoadDFResult objects containing pandas DataFrames.

#### Global Table Data

```python
from spark_rapids_tools.api_v1.builder import CSVReport

# Load global summary data
summary_result = (CSVReport(handler)
    .table("qual_summary")
    .load())

# Access the DataFrame
df = summary_result.data
print(f"Loaded {len(df)} rows of qualification data")
```

#### Per-Application Table Data

```python
# Load data for specific applications
app_results = (CSVReport(handler)
    .table("sql_to_stage_exec_info")
    .app("application_001")
    .app("application_002")
    .load())

# Load data for multiple applications at once
multi_app_results = (CSVReport(handler)
    .table("sql_to_stage_exec_info")
    .apps(["app_001", "app_002", "app_003"])
    .load())

# Load data for a single application (returns direct result)
single_app_result = (CSVReport(handler)
    .table("sql_to_stage_exec_info")
    .app("application_001")
    .load())
```

#### Advanced CSV Options

```python
# Custom pandas read arguments
custom_result = (CSVReport(handler)
    .table("exec_info")
    .pd_args({
        "sep": ",",
        "encoding": "utf-8",
        "index_col": 0,
        "parse_dates": ["timestamp"]
    })
    .load())

# Column mapping for renaming
mapped_result = (CSVReport(handler)
    .table("stage_info")
    .map_cols({
        "old_column_name": "new_column_name",
        "stage_id": "stage_identifier"
    })
    .load())

# Fallback data when table is missing
def create_fallback_data():
    import pandas as pd
    return pd.DataFrame({
        "app_id": ["fallback_app"],
        "status": ["no_data"]
    })

result_with_fallback = (CSVReport(handler)
    .table("optional_table")
    .fall_cb(create_fallback_data)
    .load())
```

### Java Properties Reports

Java Properties reports handle configuration data and metadata:

```python
from spark_rapids_tools.api_v1.builder import JPropsReport

# Load global properties
global_props = (JPropsReport(handler)
    .table("runtime.properties")
    .load())

# Load properties for specific applications
app_props = (JPropsReport(handler)
    .table("app_configuration")
    .apps(["app_001", "app_002"])
    .load())

# Load properties for a single application
single_app_props = (JPropsReport(handler)
    .table("app_configuration")
    .app("application_001")
    .load())

# Access property data
print(f"Configuration: {single_app_props.props}")
```

### TXT Reports

Text reports handle plain text data like logs and summaries:

```python
from spark_rapids_tools.api_v1.builder import TXTReport

# Load global text report
global_text = (TXTReport(handler)
    .table("qualification_summary")
    .load())

# Load text reports for multiple applications
app_texts = (TXTReport(handler)
    .table("app_logs")
    .apps(["app_001", "app_002"])
    .load())

# Load text report for a single application
single_app_text = (TXTReport(handler)
    .table("app_logs")
    .app("application_001")
    .load())

# Access text content
print(f"Log content: {single_app_text.text}")
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
from spark_rapids_tools.api_v1.builder import APIResultHandler, CSVReport, JPropsReport

# 1. Create qualification result handler
qual_handler = (APIResultHandler()
    .qual_core()
    .with_path("/data/qualification/output")
    .build())

# 2. Load qualification summary
summary = (CSVReport(qual_handler)
    .table("rapids_4_spark_qualification_output")
    .load())

print(f"Analyzed {len(summary.data)} applications")

# 3. Load detailed SQL analysis for specific apps
target_apps = ["application_001", "application_002"]
sql_analysis = (CSVReport(qual_handler)
    .table("sql_to_stage_exec_info")
    .apps(target_apps)
    .load())

# 4. Process results for each application
for app_id, result in sql_analysis.items():
    df = result.data
    print(f"App {app_id}: {len(df)} SQL operations analyzed")

    # Find unsupported operations
    unsupported = df[df['Exec Is Supported'] == False]
    if not unsupported.empty:
        print(f"  - {len(unsupported)} unsupported operations found")

# 5. Load configuration properties
props = (JPropsReport(qual_handler)
    .table("rapids_4_spark_qualification_output_properties")
    .load())

print(f"Qualification completed with configuration: {props.props}")
```

### Profiling Analysis Workflow

```python
from spark_rapids_tools.api_v1.builder import APIResultHandler, CSVReport, TXTReport

# 1. Create profiling result handler
prof_handler = (APIResultHandler()
    .prof_core()
    .with_path("s3://my-bucket/profiling/output")
    .build())

# 2. Load application information
app_info = (CSVReport(prof_handler)
    .table("application_information")
    .load())

print(f"Profiled applications: {len(app_info.data)}")

# 3. Load stage-level metrics for all applications
app_ids = app_info.data['App ID'].tolist()
stage_metrics = (CSVReport(prof_handler)
    .table("stage_level_metrics")
    .apps(app_ids)
    .pd_args({"parse_dates": ["Stage Start Time", "Stage End Time"]})
    .load())

# 4. Analyze performance bottlenecks
for app_id, result in stage_metrics.items():
    stages_df = result.data

    # Find longest running stages
    longest_stages = stages_df.nlargest(5, 'Stage Duration')
    print(f"App {app_id} - Top 5 longest stages:")
    for _, stage in longest_stages.iterrows():
        print(f"  Stage {stage['Stage ID']}: {stage['Stage Duration']}ms")

# 5. Load recommendations
recommendations = (TXTReport(prof_handler)
    .table("profile_recommendations")
    .apps(app_ids[:3])  # First 3 apps only
    .load())

for app_id, result in recommendations.items():
    print(f"Recommendations for {app_id}:")
    print(result.text)
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
