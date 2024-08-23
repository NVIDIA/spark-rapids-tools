# Spark Rapids Tools End-to-End Behavior Tests

This document outlines the end-to-end tests for Spark Rapids tools, designed to cover scenarios such as missing
dependencies, handling different types of event logs, and interacting with HDFS.

## Directory Structure
```commandline
user_tools/tests/spark_rapids_tools_e2e/
├── behave.ini                      # Configuration file for the `behave` test runner.
├── features                        # Contains test scenarios and environment setup.
│  ├── environment.py               # Setup and teardown procedures for the tests.
│  ├── steps                        # Step definitions for the tests.
│  └── test_cases.feature           # Feature file defining test scenarios.
└── resources                       # Resources used in the tests.
    ├── event_logs  
    └── scripts                     # Scripts used in the tests.  
```


## Setup

From the `<repo_root>/user_tools` directory, run the following command to install the required dependencies:


```commandline
pip install behave
or
pip install .[tests]
```


## Running Tests
Tests can be run using 'behave' cmd or using 'tox' cmd.

**Basic Usage:**

```sh
behave <options>
or
tox -e behave -- <options>
```

**Run All Tests:**

```sh
behave
or
tox -e behave
```

### Common Options

**Run Specific Tests by Tag**

```sh
behave --tags <tag>
or
tox -e behave -- --tags <tag>
```

**Run Specific Tests by Name**

```sh
behave --name <scenario_name>
or
tox -e behave -- --name <scenario_name>
```

**Skip Tests by Tag**

```sh
behave --tags ~<tag>
or
tox -e behave -- --tags ~<tag>
```

**Custom Arguments**
- Custom arguments can be passed to the behave tests using the `-D` flag.
- Example: Skip building the Tools jar during setup.

```sh
behave -D build_jar=false   # Skip building the Tools jar during setup (default: true)
or
tox -e behave -- -D build_jar=false
```



## Notes

### Initial Setup

The initial setup includes two steps:

1. Build Spark Rapids Tools JAR:
    - By default, the JAR is built before running the tests.
    - To skip this step (e.g., if the JAR is already built), use the argument -D build_jar=false.
2. Build the Python Package.

The test warns the user that initial setup may take a few minutes.

### HDFS Setup

- Some of the tests include configuring a local HDFS cluster:
- This step downloads Hadoop binaries and sets up the cluster.
  - The download occurs only once per machine but cluster setup is done for each test run.
  - Download step may take a few minutes.
- Tests involving HDFS are tagged with `@long_running` and can be skipped using `--tags ~@long_running`

### Other Details
- Included a tox environment `behave_gh-actions` to run the tests in GitHub Actions.
  - This excludes `@long_running` tests.
- Debugging Tests in IDE:
  - Intellij IDEA: Add Python configuration with module name as `behave` and working directory as the `<repo-root>/user_tools` directory.
  - VS Code: Define launch configuration (`.vscode/launch.json`) with `"module": "behave"` and `"cwd": "${workspaceFolder}/user_tools"`.
  - Ensure Python interpreter is set to the virtual environment where the tests are installed.
