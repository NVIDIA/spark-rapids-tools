# RAPIDS User Tools on Databricks Azure

This is a guide for the RAPIDS tools for Apache Spark on [Databricks Azure](https://www.databricks.com/product/azure). At the end of this guide, the user will be able to run the RAPIDS tools to analyze the clusters and the applications running on Databricks Azure.

## Assumptions

The tool currently only supports event logs stored on ABFS ([Azure Blob File System](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction-abfs-uri)). The remote output storage is also expected to be ABFS (no DBFS paths).

## Prerequisites

### 1.Databricks CLI

- Install the Databricks CLI version 0.200+. Follow the instructions on [Install the CLI](https://docs.databricks.com/en/dev-tools/cli/install.html).
- Set the configuration settings and credentials of the Databricks CLI:
  - Set up authentication by following these [instructions](https://docs.databricks.com/en/dev-tools/cli/authentication.html)
  - Verify that the access credentials are stored in the file `~/.databrickscfg` on Unix, Linux, or macOS, or in another file defined by environment variable `DATABRICKS_CONFIG_FILE`.

### 2.Azure CLI

- Install the Azure CLI. Follow the instructions on [How to install the Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli).
- Set the configuration settings and credentials of the Azure CLI:
  - Set up the authentication by following these [instructions](https://learn.microsoft.com/en-us/cli/azure/authenticate-azure-cli).
  - Configure the Azure CLI by following these [instructions](https://learn.microsoft.com/en-us/cli/azure/azure-cli-configuration).
    - `location` is used for retreving instance type description (default is `westus`).
    - Verify that the configurations are stored in the file `$AZURE_CONFIG_DIR/config` where the default value of `AZURE_CONFIG_DIR` is `$HOME/.azure` on Linux or macOS.

### 3.RAPIDS tools

- Spark event logs:
  - The RAPIDS tools can process Apache Spark CPU event logs from Spark 2.0 or higher (raw, .lz4, .lzf, .snappy, .zstd).
  - For `qualification` commands, the event logs need to be archived to an accessible local or ABFS folder.

### 4.Install the package

- Install `spark-rapids-user-tools` with python [3.8, 3.10] using:
  - pip:  `pip install spark-rapids-user-tools`
  - wheel-file: `pip install <wheel-file>`
  - from source: `pip install -e .`
- Verify the command is installed correctly by running
  ```bash
    spark_rapids_user_tools databricks-azure -- --help
  ```

### 5.Environment variables

Before running any command, you can set environment variables to specify configurations.
- RAPIDS variables have a naming pattern `RAPIDS_USER_TOOLS_*`:
  - `RAPIDS_USER_TOOLS_CACHE_FOLDER`: specifies the location of a local directory that the RAPIDS-cli uses to store and cache the downloaded resources. The default is `/var/tmp/spark_rapids_user_tools_cache`.  Note that caching the resources locally has an impact on the total execution time of the command.
  - `RAPIDS_USER_TOOLS_OUTPUT_DIRECTORY`: specifies the location of a local directory that the RAPIDS-cli uses to generate the output. The wrapper CLI arguments override that environment variable (`--local_folder` for Qualification).
- For Databricks CLI, some environment variables can be set and picked up by the RAPIDS-user tools such as: `DATABRICKS_CONFIG_FILE`, `DATABRICKS_HOST` and `DATABRICKS_TOKEN`. See the description of the variables in [Environment variables](https://docs.databricks.com/en/dev-tools/auth.html#environment-variables-and-fields-for-client-unified-authentication).
- For Azure CLI, some environment variables can be set and picked up by the RAPIDS-user tools such as: `AZURE_CONFIG_FILE` and `AZURE_DEFAULTS_LOCATION`.

## Qualification command

### Local deployment

```
spark_rapids_user_tools databricks-azure qualification [options]
spark_rapids_user_tools databricks-azure qualification -- --help
```

The local deployment runs on the local development machine. It requires:
1. Installing and configuring the Databricks and Azure CLI
2. Java 1.8+ development environment
3. Internet access to download JAR dependencies from mvn: `spark-*.jar` and `hadoop-azure-*.jar`
4. Dependencies are cached on the local disk to reduce the overhead of the download.

#### Command options

| Option                         | Description                                                                                                                                                                                                                                                                                                                                                                                               | Default                                                                                                                                                                                                                                             | Required |
|--------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:--------:|
| **cpu_cluster**                | The Databricks-cluster on which the Apache Spark applications were executed. Accepted values are an Databricks-cluster id, or a valid path to the cluster properties file (json format) generated by Databricks CLI command `databricks clusters get CLUSTER_ID [flags]`                                                                                                                                  | N/A                                                                                                                                                                                                                                                 |    N     |
| **eventlogs**                  | A comma seperated list of ABFS urls pointing to event logs or ABFS directory, or local event log filenames or directory                                                                                                                                                                                                                                                                                   | Reads the Spark's property `spark.eventLog.dir` defined in `cpu_cluster`. This property should be included in the output of `databricks clusters get CLUSTER_ID [flags]`. Note that the wrapper will raise an exception if the property is not set. |    N     |
| **remote_folder**              | The ABFS folder where the output of the wrapper's output is copied. If missing, the output will be available only on local disk                                                                                                                                                                                                                                                                           | N/A                                                                                                                                                                                                                                                 |    N     |
| **gpu_cluster**                | The Databricks-cluster on which the Spark applications is planned to be migrated. The argument can be an Databricks-cluster id or a valid path to the cluster's properties file (json format) generated by the Databricks CLI `databricks clusters get CLUSTER_ID [flags]` command                                                                                                                        | The wrapper maps the Azure machine instances of the original cluster into Azure instances that support GPU acceleration.                                                                                                                            |    N     |
| **local_folder**               | Local work-directory path to store the output and to be used as root directory for temporary folders/files. The final output will go into a subdirectory named `qual-${EXEC_ID}` where `exec_id` is an auto-generated unique identifier of the execution.                                                                                                                                                 | If the argument is NONE, the default value is the env variable `RAPIDS_USER_TOOLS_OUTPUT_DIRECTORY` if any; or the current working directory.                                                                                                       |    N     |
| **jvm_heap_size**              | The maximum heap size of the JVM in gigabytes                                                                                                                                                                                                                                                                                                                                                             | 24                                                                                                                                                                                                                                                  |    N     |
| **profile**                    | A named Databricks profile that you can specify to get the settings/credentials of the Databricks account                                                                                                                                                                                                                                                                                                 | "DEFAULT"                                                                                                                                                                                                                                           |    N     |
| **tools_jar**                  | Path to a bundled jar including RAPIDS tool. The path is a local filesystem, or remote ABFS url                                                                                                                                                                                                                                                                                                           | Downloads the latest `rapids-4-spark-tools_*.jar` from mvn repo                                                                                                                                                                                     |    N     |
| **filter_apps**                | Filtering criteria of the applications listed in the final STDOUT table is one of the following (`ALL`, `SPEEDUPS`, `SAVINGS`). "`ALL`" means no filter applied. "`SPEEDUPS`" lists all the apps that are either '_Recommended_', or '_Strongly Recommended_' based on speedups. "`SAVINGS`" lists all the apps that have positive estimated GPU savings except for the apps that are '_Not Applicable_'. | `SAVINGS`                                                                                                                                                                                                                                           |    N     |
| **gpu_cluster_recommendation** | The type of GPU cluster recommendation to generate. It accepts one of the following (`CLUSTER`, `JOB`, `MATCH`). `MATCH`: keep GPU cluster same number of nodes as CPU cluster; `CLUSTER`: recommend optimal GPU cluster by cost for entire cluster. `JOB`: recommend optimal GPU cluster by cost per job                                                                                                 | `MATCH`                                                                                                                                                                                                                                             |    N     |
| **cpu_discount**               | A percent discount for the cpu cluster cost in the form of an integer value (e.g. 30 for 30% discount)                                                                                                                                                                                                                                                                                                    | N/A                                                                                                                                                                                                                                                 |    N     |
| **gpu_discount**               | A percent discount for the gpu cluster cost in the form of an integer value (e.g. 30 for 30% discount)                                                                                                                                                                                                                                                                                                    | N/A                                                                                                                                                                                                                                                 |    N     |
| **global_discount**            | A percent discount for both the cpu and gpu cluster costs in the form of an integer value (e.g. 30 for 30% discount)                                                                                                                                                                                                                                                                                      | N/A                                                                                                                                                                                                                                                 |    N     |
| **verbose**                    | True or False to enable verbosity to the wrapper script                                                                                                                                                                                                                                                                                                                                                   | False if `RAPIDS_USER_TOOLS_LOG_DEBUG` is not set                                                                                                                                                                                                   |    N     |
| **rapids_options****           | A list of valid [Qualification tool options](../../core/docs/spark-qualification-tool.md#qualification-tool-options). Note that (`output-directory`, `platform`) flags are ignored, and that multiple "spark-property" is not supported.                                                                                                                                                                  | N/A                                                                                                                                                                                                                                                 |    N     |

#### Use case scenario

A typical workflow to successfully run the `qualification` command in local mode is described as follows:

1. Store the Apache Spark event logs in ABFS folder.
2. A user sets up his development machine:
   1. configures Java
   2. installs Databricks CLI and configures the profile and the credentials to make sure the
      access credentials are stored in the file `~/.databrickscfg` on Unix, Linux, or macOS,
      or in another file defined by environment variable `DATABRICKS_CONFIG_FILE`.
   3. installs Azure CLI and configures the credentials to make sure the Azure CLI
      commands can access the ABFS resources (i.e. storage container `LOGS_CONTAINER` which stores the event logs).
   4. installs `spark_rapids_user_tools`
3. If the results of the wrapper need to be stored on ABFS, then another ABFS uri is required `REMOTE_FOLDER=abfss://OUT_BUCKET/`
4. User defines the Databricks-cluster on which the Spark application were running. Note that the cluster does not have to be active; but it has to be visible by the Databricks CLI (i.e., can run `databricks clusters get --cluster-name`).
5. The following script runs qualification by passing an ABFS remote directory to store the output:

   ```
   # define the wrapper cache directory if necessary
   export RAPIDS_USER_TOOLS_CACHE_FOLDER=my_cache_folder
   export EVENTLOGS=abfss://LOGS_CONTAINER/eventlogs/
   export CLUSTER_ID=my-databricks-cpu-cluster-id
   export REMOTE_FOLDER=abfss://OUT_BUCKET/wrapper_output
   
   spark_rapids_user_tools databricks-azure qualification \
      --eventlogs $EVENTLOGS \
      --cpu_cluster $CLUSTER_ID \
      --remote_folder $REMOTE_FOLDER
   ```
   The wrapper generates a unique-Id for each execution in the format of `qual_<YYYYmmddHHmmss>_<0x%08X>`
   The above command will generate a directory containing `qualification_summary.csv` in addition to
   the actual folder of the RAPIDS Qualification tool. The directory will be mirrored to ABFS path (`REMOTE_FOLDER`).

   ```
    ./qual_<YYYYmmddHHmmss>_<0x%08X>/qualification_summary.csv
    ./qual_<YYYYmmddHHmmss>_<0x%08X>/rapids_4_spark_qualification_output/
   ```

### Qualification output

For each app, the command output lists the following fields:

- `App ID`: An application is referenced by its application ID, '_app-id_'. When running on YARN,
  each application may have multiple attempts, but there are attempt IDs only for applications
  in cluster mode, not applications in client mode.  Applications in YARN cluster mode can be
  identified by their attempt-id.
- `App Name`: Name of the application
- `Speedup Based Recommendation`: Recommendation based on '_Estimated Speed-up Factor_'. Note that an
  application that has job or stage failures will be labeled '_Not Applicable_'
- `Savings Based Recommendation`: Recommendation based on '_Estimated GPU Savings_'.
    - '_Strongly Recommended_': An app with savings GEQ 40%
    - '_Recommended_': An app with savings between (1, 40) %
    - '_Not Recommended_': An app with no savings
    - '_Not Applicable_': An app that has job or stage failures.
- `Estimated GPU Speedup`: Speed-up factor estimated for the app. Calculated as the ratio
  between '_App Duration_' and '_Estimated GPU Duration_'.
- `Estimated GPU Duration`: Predicted runtime of the app if it was run on GPU
- `App Duration`: Wall-Clock time measured since the application starts till it is completed.
  If an app is not completed an estimated completion time would be computed.
- `Estimated GPU Savings(%)`: Percentage of cost savings of the app if it migrates to an
  accelerated cluster. It is calculated as:
  ```
  estimated_saving = 100 - ((100 * gpu_cost) / cpu_cost)
  ```

The command creates a directory with UUID that contains the following:
- Directory generated by the RAPIDS qualification tool `rapids_4_spark_qualification_output`;
- A CSV file that contains the summary of all the applications along with estimated absolute costs
- Sample directory structure: 
    ```
    .
    ├── qualification_summary.csv
    └── rapids_4_spark_qualification_output
        ├── rapids_4_spark_qualification_output.csv
        ├── rapids_4_spark_qualification_output.log
        ├── rapids_4_spark_qualification_output_execs.csv
        ├── rapids_4_spark_qualification_output_stages.csv
        └── ui
    ```

## Profiling command

### Local deployment

```
spark_rapids_user_tools databricks-azure profiling [options]
spark_rapids_user_tools databricks-azure profiling -- --help
```

The local deployment runs on the local development machine. It requires:
1. Installing and configuring the Databricks and Azure CLI
2. Java 1.8+ development environment
3. Internet access to download JAR dependencies from mvn: `spark-*.jar`, and `hadoop-azure-*.jar`
4. Dependencies are cached on the local disk to reduce the overhead of the download.

#### Command options

| Option               | Description                                                                                                                                                                                                                                                            | Default                                                                                                                                                                                                                                         | Required |
|----------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| **gpu_cluster**      | The Databricks-cluster on which the Apache Spark applications were executed. Accepted values are an Databricks-cluster id, or a valid path to the cluster properties file (json format) generated by Databricks CLI command `databricks clusters get CLUSTER_ID [flags]` | If missing, then the argument `worker_info` has to be provided.                                                                                                                                                                                 |     N    |
| **worker_info**      | A path pointing to a yaml file containing the system information of a worker node. It is assumed that all workers are homogenous. The format of the file is described in the following section.                                                                        | None                                                                                                                                                                                                                                            |     N    |
| **eventlogs**        | A comma seperated list of ABFS urls pointing to event logs or ABFS directory                                                                                                                                                                                           | Reads the Spark's property `spark.eventLog.dir` defined in `gpu_cluster`. This property should be included in the output of `databricks clusters get CLUSTER_ID [flags]`. Note that the wrapper will raise an exception if the property is not set. |     N    |
| **remote_folder**    | The ABFS folder where the output of the wrapper's output is copied. If missing, the output will be available only on local disk                                                                                                                                        | N/A                                                                                                                                                                                                                                             |     N    |
| **local_folder**     | Local work-directory path to store the output and to be used as root directory for temporary folders/files. The final output will go into a subdirectory named `prof-${EXEC_ID}` where `exec_id` is an auto-generated unique identifier of the execution.              | If the argument is NONE, the default value is the env variable `RAPIDS_USER_TOOLS_OUTPUT_DIRECTORY` if any; or the current working directory.                                                                                                   |     N    |
| **profile**          | A named Databricks profile that you can specify to get the settings/credentials of the Databricks account                                                                                                                                                              | "DEFAULT"                                                                                                                                                                                                                                       |     N    |
| **jvm_heap_size**    | The maximum heap size of the JVM in gigabytes                                                                                                                                                                                                                          | 24                                                                                                                                                                                                                                              |     N    |
| **tools_jar**        | Path to a bundled jar including RAPIDS tool. The path is a local filesystem, or remote ABFS url                                                                                                                                                                        | Downloads the latest `rapids-4-spark-tools_*.jar` from mvn repo                                                                                                                                                                                 |     N    |
| **credentials_file** | The local path of JSON file that contains the application credentials                                                                                                                                                                                                  | If missing, loads the env variable `DATABRICKS_CONFIG_FILE` if any. Otherwise, it uses the default path `~/.databrickscfg` on Unix, Linux, or macOS                                                                                             |     N    |
| **verbose**          | True or False to enable verbosity to the wrapper script                                                                                                                                                                                                                | False if `RAPIDS_USER_TOOLS_LOG_DEBUG` is not set                                                                                                                                                                                               |     N    |
| **rapids_options**** | A list of valid [Profiling tool options](../../core/docs/spark-profiling-tool.md#qualification-tool-options). Note that (`output-directory`, `auto-tuner`, `combined`) flags are ignored                                                                               | N/A                                                                                                                                                                                                                                             |     N    |

If the CLI does not provide an argument `gpu_cluster`, then a valid path to yaml file must be
provided through the arg `worker_info`.
The `worker_info` is a yaml file that contains the HW description of the workers. It must contain
the following properties:
- `system.numCores`: number of cores of a single worker node
- `system.memory`: RAM size in MiB of a single node
- `system.numWorkers`: number of workers
- `gpu.name`: the accelerator installed on the worker node
- `gpu.memory`: memory size of the accelerator in MiB. (i.e., 16GB for Nvidia-T4)
- `softwareProperties`: Spark default-configurations of the target cluster

An example of valid `worker_info.yaml`:

  ```
  system:
    numCores: 32
    memory: 212992MiB
    numWorkers: 5
  gpu:
    memory: 15109MiB
    count: 4
    name: T4
  softwareProperties:
    spark.driver.maxResultSize: 7680m
    spark.driver.memory: 15360m
    spark.executor.cores: '8'
    spark.executor.instances: '2'
    spark.executor.memory: 47222m
    spark.executorEnv.OPENBLAS_NUM_THREADS: '1'
    spark.scheduler.mode: FAIR
    spark.sql.cbo.enabled: 'true'
    spark.ui.port: '0'
    spark.yarn.am.memory: 640m
  ```

#### Use case scenario

A typical workflow to successfully run the `profiling` command in local mode is described as follows:

1. Store the Apache Spark event logs in ABFS folder.
2. A user sets up his development machine:
   1. configures Java
   2. installs Databricks CLI and configures the profile and the credentials to make sure the
      access credentials are stored in the file `~/.databrickscfg` on Unix, Linux, or macOS,
      or in another file defined by environment variable `DATABRICKS_CONFIG_FILE`.
   3. installs Azure CLI and configures the credentials to make sure the Azure CLI
      commands can access the ABFS resources (i.e. storage container `LOGS_CONTAINER` which stores the event logs).
   4. installs `spark_rapids_user_tools`
3. If the results of the wrapper need to be stored on ABFS, then another ABFS uri is required `REMOTE_FOLDER=abfss://OUT_BUCKET/`
4. Depending on the accessibility of the cluster properties, the user chooses one of the 2 cases below (_"Case-A"_, and _"Case-B"_) to trigger the CLI.

For each successful execution, the wrapper generates a new directory in the format of
`prof_<YYYYmmddHHmmss>_<0x%08X>`. The directory contains `profiling_summary.log` in addition to
the actual folder of the RAPIDS Profiling tool. The directory will be mirrored to ABFS folder if the
argument `--remote_folder` was a valid ABFS path.

   ```
    ./prof_<YYYYmmddHHmmss>_<0x%08X>/profiling_summary.log
    ./prof_<YYYYmmddHHmmss>_<0x%08X>/rapids_4_spark_profile/
   ```

**Case-A: A gpu-cluster property file is accessible:**

A cluster property is still accessible if one of the following conditions applies:

1. The cluster is listed by the `databricks clusters get CLUSTER_ID [flags]` cmd. In this case, the CLI will be triggered by providing
   `--gpu_cluster $CLUSTER_ID`

       ```
       # run the command using the GPU cluster name
       export RAPIDS_USER_TOOLS_CACHE_FOLDER=my_cache_folder
       export EVENTLOGS=abfss://LOGS_CONTAINER/eventlogs/
       export CLUSTER_ID=my-databricks-gpu-cluster-id
       export REMOTE_FOLDER=abfss://OUT_BUCKET/wrapper_output
       
       spark_rapids_user_tools databricks-azure profiling \
          --eventlogs $EVENTLOGS \
          --gpu_cluster $CLUSTER_ID \
          --remote_folder $REMOTE_FOLDER
       ```
2. The cluster properties file is accessible on local disk or a valid ABFS path.

   ```
   $> export CLUSTER_PROPS_FILE=cluster-props.json
   $> databricks clusters get $CLUSTER_ID > $CLUSTER_PROPS_FILE
   ```
   Trigger the CLI by providing the path to the properties file `--gpu_cluster $CLUSTER_PROPS_FILE`

   ```
   $> spark_rapids_user_tools databricks-azure profiling \
        --eventlogs $EVENTLOGS \
        --gpu_cluster $CLUSTER_PROPS_FILE \
        --remote_folder $REMOTE_FOLDER
   ```

**Case-B: GPU cluster information is missing:**

In this scenario, users can write down a simple yaml file to describe the shape of the worker nodes.  
This case is relevant to the following plans:
1. Users who might want to experiment with different configurations before deciding on the final
   cluster shape. 
2. Users who have no access to the properties of the cluster.

The CLI is triggered by providing the location where the yaml file is stored `--worker_info $WORKER_INFO_PATH`

    ```
    # First, create a yaml file as described in previous section
    $> export WORKER_INFO_PATH=worker-info.yaml
    # Run the profiling cmd
    $> spark_rapids_user_tools databricks-azure profiling \
            --eventlogs $EVENTLOGS \
            --worker_info $WORKER_INFO_PATH \
            --remote_folder $REMOTE_FOLDER
    ```


## Diagnostic command

```
spark_rapids_user_tools databricks-azure diagnostic [options]
spark_rapids_user_tools databricks-azure diagnostic --help
```

Run diagnostic command to collects information from Databricks cluster, such as OS version, # of worker
nodes, Yarn configuration, Spark version and error logs etc. The cluster has to be running and the
user must have SSH access.

### Diagnostic options

| Option            | Description                                                                                               | Default                                                                                     | Required |
|-------------------|-----------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------|:--------:|
| **cluster**       | Id of the Databricks cluster running an accelerated computing instance                                    | N/A                                                                                         |    Y     |
| **profile**       | A named Databricks profile that you can specify to get the settings/credentials of the Databricks account | "DEFAULT"                                                                                   |    N     |
| **output_folder** | Path to local directory where the final recommendations is logged                                         | env variable `RAPIDS_USER_TOOLS_OUTPUT_DIRECTORY` if any; or the current working directory. |    N     |
| **port**          | Port number to be used for the ssh connections                                                            | 2200                                                                                        |    N     |
| **key_file**      | Path to the private key file to be used for the ssh connections.                                          | Default ssh key based on the OS                                                             |    N     |
| **thread_num**    | Number of threads to access remote cluster nodes in parallel                                              | 3                                                                                           |    N     |
| **yes**           | auto confirm to interactive question                                                                      | False                                                                                       |    N     |
| **verbose**       | True or False to enable verbosity to the wrapper script                                                   | False if `RAPIDS_USER_TOOLS_LOG_DEBUG` is not set                                           |    N     |

### Info collection

The default is to collect info from each cluster node via SSH access and an archive would be created
to output folder at last.

The steps to run the command:

1. The user creates a cluster
2. The user runs the following command:

    ```bash
    spark_rapids_user_tools databricks-azure diagnostic \
      --cluster my-cluster-id
    ```
   
If the connection to Databricks instances cannot be established through SSH, the command will raise error.