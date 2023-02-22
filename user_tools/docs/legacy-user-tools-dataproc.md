# RAPIDS User Tools on Dataproc (Deprecated)

Note: This documentation is for an older version of the spark-rapids-user-tools wrapper. The support
for the `spark_rapids_dataproc` is limited to bug fixes.  
For more information, see the [documentation](user-tools-dataproc.md) that describes the usage `spark_rapids_user_tools dataproc` command.


## Prerequisites

Make sure to install gcloud SDK if you plan to run the tool wrapper by following the references below:
- [Install the Google Cloud CLI](https://cloud.google.com/sdk/docs/install-sdk)
- [Initialize the gcloud CLI](https://cloud.google.com/sdk/docs/install-sdk#initializing_the)

## Using the Rapids Tool Wrapper

The wrapper provides convenient way to run Qualification/Profiling tool.
Default properties can be set in two files `qualification-conf.yaml` and `profiling-conf.yaml` under
"resources" directory.

- run the help command `spark_rapids_dataproc --help`

  ```bash
  NAME
      spark_rapids_dataproc - A wrapper script to run Rapids Qualification/Profiling tools on DataProc

  SYNOPSIS
      spark_rapids_dataproc <TOOL> - where tool is one of following: qualification, profiling and boostrap
      For details on the argument of each tool
      spark_rapids_dataproc <TOOL> --help

  DESCRIPTION
      Disclaimer:
        Estimates provided by the tools are based on the currently supported "SparkPlan" or
        "Executor Nodes" used in the application. It currently does not handle all the expressions
        or datatypes used.
        The pricing estimate does not take into considerations:
        1- Sustained Use discounts
        2- Cost of on-demand VMs

  ```

### Qualification Tool

The Qualification tool analyzes Spark events generated from  CPU based Spark applications to help
quantify the expected acceleration and costs savings of migrating a Spark application or
query to GPU.  
For more details, please visit the
[Qualification Tool on Github pages](https://nvidia.github.io/spark-rapids/docs/spark-qualification-tool.html).

#### Sample commands

- run the qualification tool help cmd `spark_rapids_dataproc qualification --help`
    ```
    NAME
        spark_rapids_dataproc qualification - The Qualification tool analyzes Spark events generated from
        CPU based Spark applications to help quantify the expected acceleration and costs savings of migrating
        a Spark application or query to GPU.
    
    SYNOPSIS
        spark_rapids_dataproc qualification CLUSTER REGION <flags>
    
    DESCRIPTION
        Disclaimer:
            Estimates provided by the Qualification tool are based on the currently supported "SparkPlan" or
            "Executor Nodes" used in the application.
            It currently does not handle all the expressions or datatypes used.
            Please refer to "Understanding Execs report" section and the "Supported Operators" guide
            to check the types and expressions you are using are supported.
    
    POSITIONAL ARGUMENTS
        CLUSTER
            Type: str
            Name of the dataproc cluster on which the Qualification tool is executed. Note that the cluster
            has to: 1- be running; and 2- support Spark3.x+.
        REGION
            Type: str
            Compute region (e.g. us-central1) for the cluster.
    
    FLAGS
        --tools_jar=TOOLS_JAR
            Type: Optional[str]
            Default: None
            Path to a bundled jar including Rapids tool. The path is a local filesystem, or gstorage url.
        --eventlogs=EVENTLOGS
            Type: Optional[str]
            Default: None
            Event log filenames(comma separated) or gcloud storage directories containing event logs.
            eg: gs://<BUCKET>/eventlog1,gs://<BUCKET1>/eventlog2 If not specified, the wrapper will pull
            the default SHS directory from the cluster properties, which is equivalent to
            gs://$temp_bucket/$uuid/spark-job-history or the PHS log directory if any.
        --output_folder=OUTPUT_FOLDER
            Type: str
            Default: '.'
            Base output directory. The final output will go into a subdirectory called wrapper-output.
            It will overwrite any existing directory with the same name.
        --filter_apps=FILTER_APPS
            Type: str
            Default: 'savings'
            [NONE | recommended | savings] filtering criteria of the applications listed in the final
            STDOUT table. Note that this filter does not affect the CSV report. “NONE“ means no filter
            applied. “recommended“ lists all the apps that are either 'Recommended', or
            'Strongly Recommended'. “savings“ lists all the apps that have positive estimated GPU savings.
        --gpu_device=GPU_DEVICE
            Type: str
            Default: 'T4'
            The type of the GPU to add to the cluster. Options are [T4, V100, K80, A100, P100].
        --gpu_per_machine=GPU_PER_MACHINE
            Type: int
            Default: 2
            The number of GPU accelerators to be added to each VM image.
        --cuda=CUDA
            Type: str
            Default: '11.5'
            cuda version to be used with the GPU accelerator.
        --cpu_cluster_props=CPU_CLUSTER_PROPS
            Type: Optional[str]
            Default: None
            Path to a file (json/yaml) containing configurations of the CPU cluster on which the Spark applications
            were executed.
            The path is a local filesystem, or gstorage url.
            This option does not require the cluster to be live.
            When missing, the configurations are pulled from the live cluster on which the
            Qualification tool is submitted.
        --cpu_cluster_region=CPU_CLUSTER_REGION
            Type: Optional[str]
            Default: None
            The region where the CPU cluster belongs to. Note that this parameter requires 'cpu_cluster_props' to be
            defined.
            When missing, the region is set to the value passed in the 'region' argument.
        --cpu_cluster_zone=CPU_CLUSTER_ZONE
            Type: Optional[str]
            Default: None
            The zone where the CPU cluster belongs to. Note that this parameter requires 'cpu_cluster_props' to be
            defined.
            When missing, the zone is set to the same zone as the 'cluster' on which the Qualification tool is submitted.
        --gpu_cluster_props=GPU_CLUSTER_PROPS
            Type: Optional[str]
            Default: None
            Path of a file (json/yaml) containing configurations of the GPU cluster on which the Spark
            applications is planned to be migrated.
            The path is a local filesystem, or gstorage url.
            This option does not require the cluster to be live.
            When missing, the configurations are considered the same as the ones used by the 'cpu_cluster_props'.
        --gpu_cluster_region=GPU_CLUSTER_REGION
            Type: Optional[str]
            Default: None
            The region where the GPU cluster belongs to. Note that this parameter requires 'gpu_cluster_props' to be
            defined.
            When missing, the region is set to the value passed in the 'region' argument.
        --gpu_cluster_zone=GPU_CLUSTER_ZONE
            Type: Optional[str]
            Default: None
            The zone where the GPU cluster belongs to. Note that this parameter requires 'gpu_cluster_props' to be
            defined.
            When missing, the zone is set to the same zone as the 'cluster' on which the Qualification tool is submitted.
        --debug=DEBUG
            Type: bool
            Default: False
            True or False to enable verbosity to the wrapper script.
        Additional flags are accepted.
            A list of valid Qualification tool options. Note that the wrapper ignores the “output-directory“
            flag, and it does not support multiple “spark-property“ arguments. For more details on
            Qualification tool options, please visit https://nvidia.github.io/spark-rapids/docs/spark-qualification-tool.html#qualification-tool-options.
    
    ```

- Example: Running Qualification tool on cluster that does not support Spark3.x
    ```
      spark_rapids_dataproc qualification \
            --cluster dp-spark2 \
            --region us-central1 \
            --cpu_cluster_props=/tmp/test_cpu_cluster_prop.yaml \
            --gpu_cluster_props=/tmp/test_gpu_cluster_prop_e2.yaml \
            --eventlogs=gs://my-bucket/qualification_testing/dlrm_cpu/,gs://my-bucket/qualification_testing/tpcds_100in1/
      2022-11-04 16:27:44,196 WARNING qualification: The cluster image 1.5.75-debian10 is not supported. To support the RAPIDS user tools, you will need to use an image that runs Spark3.x.
      Failure Running Rapids Tool.
              Tool cannot execute on the execution cluster.
              Run Terminated with error.
              The cluster image 1.5.75-debian10 is not supported. To support the RAPIDS user tools, you will need to use an image that runs Spark3.x.
    ```

- Example: Running Qualification tool passing list of google storage directories
    - Note that the wrapper lists the applications with positive recommendations.
      To list all the applications, set the argument `--filter_apps=NONE`
    - cmd
      ```
      spark_rapids_dataproc \
          qualification \
          --cluster=jobs-test-003 \
          --region=us-central1 \
          --eventlogs=gs://my-bucket/qualification_testing/dlrm_cpu/,gs://my-bucket/qualification_testing/tpcds_100in1/
      ```
    - result
      ```
      Qualification tool output is saved to local disk /data/repos/issues/umbrella-dataproc/repos/issues/spark-rapids-tools-35-b/wrapper-output/spark_rapids_dataproc_qualification/qual-tool-output/rapids_4_spark_qualification_output
              rapids_4_spark_qualification_output/
                      ├── rapids_4_spark_qualification_output.log
                      ├── rapids_4_spark_qualification_output.csv
                      ├── rapids_4_spark_qualification_output_execs.csv
                      ├── rapids_4_spark_qualification_output_stages.csv
                      └── ui/
      - To learn more about the output details, visit https://nvidia.github.io/spark-rapids/docs/spark-qualification-tool.html#understanding-the-qualification-tool-output
      Full savings and speedups CSV report: /data/repos/issues/umbrella-dataproc/repos/issues/spark-rapids-tools-35-b/wrapper-output/spark_rapids_dataproc_qualification/qual-tool-output/rapids_4_dataproc_qualification_output.csv
      +----+-------------------------+---------------------+----------------------+-----------------+-----------------+---------------+-----------------+
      |    | App ID                  | App Name            | Recommendation       |   Estimated GPU |   Estimated GPU |           App |   Estimated GPU |
      |    |                         |                     |                      |         Speedup |     Duration(s) |   Duration(s) |      Savings(%) |
      |----+-------------------------+---------------------+----------------------+-----------------+-----------------+---------------+-----------------|
      |  0 | app-20200423035604-0002 | spark_data_utils.py | Strongly Recommended |            3.66 |          651.24 |       2384.32 |           64.04 |
      |  1 | app-20200423035119-0001 | spark_data_utils.py | Strongly Recommended |            3.14 |           89.61 |        281.62 |           58.11 |
      |  2 | app-20200423033538-0000 | spark_data_utils.py | Strongly Recommended |            3.12 |          300.39 |        939.21 |           57.89 |
      |  3 | app-20210509200722-0001 | Spark shell         | Strongly Recommended |            2.55 |          698.16 |       1783.65 |           48.47 |
      +----+-------------------------+---------------------+----------------------+-----------------+-----------------+---------------+-----------------+
      Report Summary:
      ------------------------------  ------
      Total applications                   4
      RAPIDS candidates                    4
      Overall estimated speedup         3.10
      Overall estimated cost savings  57.50%
      ------------------------------  ------
      To launch a GPU-accelerated cluster with RAPIDS Accelerator for Apache Spark, add the following to your cluster creation script:
              --initialization-actions=gs://goog-dataproc-initialization-actions-us-central1/gpu/install_gpu_driver.sh,gs://goog-dataproc-initialization-actions-us-central1/rapids/rapids.sh \ 
              --worker-accelerator type=nvidia-tesla-t4,count=2 \ 
              --metadata gpu-driver-provider="NVIDIA" \ 
              --metadata rapids-runtime=SPARK \ 
              --cuda-version=11.5
      ```


- Example: Running Qualification tool a passing list of google storage directories when cluster is running a n2 instance. N2 instances don't support GPU at the time of writing this tool and so the tool will recommend an equivalent n1 instance and run the qualification using that instance.
    - Note that the wrapper lists the applications with positive recommendations.
      To list all the applications, set the argument `--filter_apps=NONE`.
    - cmd
      ```
      spark_rapids_dataproc \
          qualification \
          --cluster=dataproc-wrapper-test \
          --region=us-central1 \
          --eventlogs=gs://my-bucket/qualification_testing/dlrm_cpu/,gs://my-bucket/qualification_testing/tpcds_100in1/
      ```
    - result
      ```
      Qualification tool output is saved to local disk /data/repos/issues/umbrella-dataproc/repos/issues/spark-rapids-tools-35-b/wrapper-output/spark_rapids_dataproc_qualification/qual-tool-output/rapids_4_spark_qualification_output
              rapids_4_spark_qualification_output/
                      ├── rapids_4_spark_qualification_output.log
                      ├── rapids_4_spark_qualification_output.csv
                      ├── rapids_4_spark_qualification_output_execs.csv
                      ├── rapids_4_spark_qualification_output_stages.csv
                      └── ui/
      - To learn more about the output details, visit https://nvidia.github.io/spark-rapids/docs/spark-qualification-tool.html#understanding-the-qualification-tool-output
      Full savings and speedups CSV report: /data/repos/issues/umbrella-dataproc/repos/issues/spark-rapids-tools-35-b/wrapper-output/spark_rapids_dataproc_qualification/qual-tool-output/rapids_4_dataproc_qualification_output.csv
      +----+---------------------+-------------------------+----------------------+-----------------+-----------------+---------------+-----------------+
      |    | App Name            | App ID                  | Recommendation       |   Estimated GPU |   Estimated GPU |           App |   Estimated GPU |
      |    |                     |                         |                      |         Speedup |     Duration(s) |   Duration(s) |      Savings(%) |
      |----+---------------------+-------------------------+----------------------+-----------------+-----------------+---------------+-----------------|
      |  0 | spark_data_utils.py | app-20200423035604-0002 | Strongly Recommended |            3.04 |          783.38 |       2384.32 |           27.25 |
      |  1 | spark_data_utils.py | app-20200423033538-0000 | Strongly Recommended |            2.86 |          327.36 |        939.21 |           22.82 |
      |  2 | spark_data_utils.py | app-20200423035119-0001 | Strongly Recommended |            2.69 |          104.35 |        281.62 |           17.95 |
      |  3 | Spark shell         | app-20210509200722-0001 | Recommended          |            2.25 |          789.90 |       1783.65 |            1.94 |
      +----+---------------------+-------------------------+----------------------+-----------------+-----------------+---------------+-----------------+
      Report Summary:
      ------------------------------  ------
      Total applications                   4
      RAPIDS candidates                    4
      Overall estimated acceleration    3.10
      Overall estimated cost savings  57.50%
      ------------------------------  ------
      
      To support acceleration with T4 GPUs, you will need to switch your worker node instance type to n1-highcpu-32
      To launch a GPU-accelerated cluster with RAPIDS Accelerator for Apache Spark, add the following to your cluster creation script:
              --initialization-actions=gs://goog-dataproc-initialization-actions-us-central1/gpu/install_gpu_driver.sh,gs://goog-dataproc-initialization-actions-us-central1/rapids/rapids.sh \ 
              --worker-accelerator type=nvidia-tesla-t4,count=2 \ 
              --metadata gpu-driver-provider="NVIDIA" \ 
              --metadata rapids-runtime=SPARK \ 
              --cuda-version=11.5
      ```

#### Running Qualification Tool with offline CPU/GPU clusters

Users can pass configuration files that describe the clusters involved in migrations. The files can
be stored locally or on GCS bucket.

- Format of the cluster configuration:
    - Both Json and Yaml formats are accepted
    - For the top level entry of configurations, both `config` and `cluster_config` are accepted.
    - `config` or `cluster_config` does not have to be the first entry in the yaml/json file. It can be
      nested.
    - For keys, the wrapper accepts camelCase and underscores keywords. i.e., `cluster_config` and `clusterConfig`
      are both accepted.
    - For each node type (master/worker), the configuration file has to indicate the following:
        - `machine_type_uri` or `machineTypeUri`
        - `num_instances` or `numInstances`
        - `image_version` or `imageVersion`
    - Optional configurations:
        - `num_local_ssds` or `numLocalSsds`. For GPU clusters, the value will be set to 1 when the config is missing.
        - `accelerators` configuration is ignored since the wrapper command line overrides the GPU accelerators.
    - example of accepted files:
        - json file
          ```json
              {
                "cluster_config": {
                  "software_config": {
                    "image_version": "1.5"
                  },
                  "gce_cluster_config": {
                    "metadata": {
                      "enable-pepperdata": "true"
                    }
                  },
                  "master_config": {
                    "machine_type_uri": "n1-standard-16",
                    "disk_config": {
                      "boot_disk_size_gb": 100
                    }
                  },
                  "worker_config": {
                    "num_instances": 2,
                    "machine_type_uri": "n1-standard-16",
                    "disk_config": {
                      "boot_disk_size_gb": 100
                    }
                  },
                  "secondary_worker_config": {
                    "num_instances": 2,
                    "machine_type_uri": "n1-standard-16",
                    "disk_config": {
                      "boot_disk_size_gb": 100
                    }
                  }
                }
              }
          ```
        - yaml file
          ```yaml
              cluster_config:
                gce_cluster_config:
                  metadata:
                    enable-pepperdata: 'true'
                master_config:
                  disk_config:
                    boot_disk_size_gb: 100
                  machine_type_uri: n1-standard-16
                secondary_worker_config:
                  disk_config:
                    boot_disk_size_gb: 100
                  machine_type_uri: n1-standard-16
                  num_instances: 2
                software_config:
                  image_version: '1.5'
                worker_config:
                  disk_config:
                    boot_disk_size_gb: 100
                  machine_type_uri: n1-standard-16
                  num_instances: 2
          ```
        - The cluster configs are not at top level.
          ```yaml
            key_level0:
              key_level1:
                cluster_config:
                  gce_cluster_config:
                    metadata:
                      enable-pepperdata: 'true'
                  master_config:
                    disk_config:
                      boot_disk_size_gb: 100
                    machine_type_uri: n1-standard-16
                  secondary_worker_config:
                    disk_config:
                      boot_disk_size_gb: 100
                    machine_type_uri: n1-standard-16
                    num_instances: 2
                  software_config:
                    image_version: '1.5'
                  worker_config:
                    disk_config:
                      boot_disk_size_gb: 100
                    machine_type_uri: n1-standard-16
                    num_instances: 2
          ```
        - Camel Case file is accepted:
          ```yaml
            clusterConfig:
              gceClusterConfig:
                metadata:
                  enable-pepperdata: 'true'
              masterConfig:
                diskConfig:
                  bootDiskSizeGb: 100
                machineTypeUri: n1-standard-16
              secondaryWorkerConfig:
                diskConfig:
                  bootDiskSizeGb: 100
                machineTypeUri: n1-standard-16
                numInstances: 2
              softwareConfig:
                imageVersion: '2.0'
              workerConfig:
                diskConfig:
                  bootDiskSizeGb: 100
                machineTypeUri: n1-standard-16
                numInstances: 2
          ```

            - example `cpu_cluster_props` argument passed to CLI
              ```
                spark_rapids_dataproc qualification \
                           --cluster jobs-test-gpu-support \
                           --region us-central1 --cpu_cluster_props=/tmp/test_cpu_cluster_prop.yaml \
                           --eventlogs=gs://my-bucket/qualification_testing/dlrm_cpu/,gs://my-bucket/qualification_testing/tpcds_100in1/
          
                2022-11-04 17:11:02,284 INFO qualification: The CPU cluster is an offline cluster. Properties are loaded from /data/repos/issues/umbrella-dataproc/repos/issues/spark-rapids-tools-63/test_cpu_cluster_prop.yaml
                2022-11-04 17:11:02,286 WARNING qualification: The cluster image 1.5 is not supported. To support the RAPIDS user tools, you will need to use an image that runs Spark3.x.
                2022-11-04 17:11:02,288 INFO qualification: The GPU cluster is the same as the original CPU cluster properties loaded from /data/repos/issues/umbrella-dataproc/repos/issues/spark-rapids-tools-63/test_cpu_cluster_prop.yaml.
                    To update the configuration of the GPU cluster, make sure to pass the properties file to the CLI arguments
                2022-11-04 17:12:29,398 INFO qualification: Downloading the price catalog from URL https://cloudpricingcalculator.appspot.com/static/data/pricelist.json
                2022-11-04 17:12:30,292 INFO qualification: Generating GPU Estimated Speedup and Savings as ./wrapper-output/rapids_user_tools_qualification/qual-tool-output/rapids_4_dataproc_qualification_output.csv
                Qualification tool output is saved to local disk /data/repos/issues/umbrella-dataproc/repos/issues/spark-rapids-tools-63/wrapper-output/rapids_user_tools_qualification/qual-tool-output/rapids_4_spark_qualification_output
                        rapids_4_spark_qualification_output/
                                └── ui/
                                ├── rapids_4_spark_qualification_output_stages.csv
                                ├── rapids_4_spark_qualification_output.csv
                                ├── rapids_4_spark_qualification_output_execs.csv
                                ├── rapids_4_spark_qualification_output.log
                - To learn more about the output details, visit https://nvidia.github.io/spark-rapids/docs/spark-qualification-tool.html#understanding-the-qualification-tool-output
                Full savings and speedups CSV report: /data/repos/issues/umbrella-dataproc/repos/issues/spark-rapids-tools-63/wrapper-output/rapids_user_tools_qualification/qual-tool-output/rapids_4_dataproc_qualification_output.csv
                +----+-------------------------+---------------------+----------------------+-----------------+-----------------+---------------+-----------------+
                |    | App ID                  | App Name            | Recommendation       |   Estimated GPU |   Estimated GPU |           App |   Estimated GPU |
                |    |                         |                     |                      |         Speedup |     Duration(s) |   Duration(s) |      Savings(%) |
                |----+-------------------------+---------------------+----------------------+-----------------+-----------------+---------------+-----------------|
                |  0 | app-20200423035604-0002 | spark_data_utils.py | Strongly Recommended |            3.66 |          651.24 |       2384.32 |           25.04 |
                |  1 | app-20200423035119-0001 | spark_data_utils.py | Strongly Recommended |            3.14 |           89.61 |        281.62 |           12.68 |
                |  2 | app-20200423033538-0000 | spark_data_utils.py | Strongly Recommended |            3.12 |          300.39 |        939.21 |           12.23 |
                +----+-------------------------+---------------------+----------------------+-----------------+-----------------+---------------+-----------------+
                Report Summary:
                ------------------------------  ------
                Total applications                   4
                RAPIDS candidates                    4
                Overall estimated speedup         3.13
                Overall estimated cost savings  12.30%
                ------------------------------  ------
                To launch a GPU-accelerated cluster with RAPIDS Accelerator for Apache Spark, add the following to your cluster creation script:
                        --initialization-actions=gs://goog-dataproc-initialization-actions-us-central1/gpu/install_gpu_driver.sh,gs://goog-dataproc-initialization-actions-us-central1/rapids/rapids.sh \ 
                        --worker-accelerator type=nvidia-tesla-t4,count=2 \ 
                        --metadata gpu-driver-provider="NVIDIA" \ 
                        --metadata rapids-runtime=SPARK \ 
                        --cuda-version=11.5
              ```

### Profiling Tool

The Profiling tool analyzes both CPU or GPU generated event logs and generates information which
can be used for debugging and profiling Apache Spark applications.  
In addition, the wrapper output provides optimized RAPIDS configurations based on the worker's
information.  
For more details, please visit the
[Profiling Tool on Github pages](https://nvidia.github.io/spark-rapids/docs/spark-profiling-tool.html).

#### Sample commands

- run the profiling tool help cmd `spark_rapids_dataproc profiling --help`
  ```bash
  NAME
      spark_rapids_dataproc profiling - The Profiling tool analyzes both CPU or GPU generated event
      logs and generates information which can be used for debugging and profiling Apache Spark applications.

  SYNOPSIS
      spark_rapids_dataproc profiling CLUSTER REGION <flags>

  DESCRIPTION
      The output information contains the Spark version, executor details, properties, etc. It also
      uses heuristics based techniques to recommend Spark configurations for users to run Spark on RAPIDS.

  POSITIONAL ARGUMENTS
      CLUSTER
          Type: str
          Name of the dataproc cluster
      REGION
          Type: str
          Compute region (e.g. us-central1) for the cluster.

  FLAGS
      --tools_jar=TOOLS_JAR
          Type: Optional[str]
          Default: None
          Path to a bundled jar including Rapids tool. The path is a local filesystem, or gstorage url.
      --eventlogs=EVENTLOGS
          Type: Optional[str]
          Default: None
          Event log filenames(comma separated) or gcloud storage directories containing event logs.
          eg: gs://<BUCKET>/eventlog1,gs://<BUCKET1>/eventlog2 If not specified, the wrapper will pull
          the default SHS directory from the cluster properties, which is equivalent to
          gs://$temp_bucket/$uuid/spark-job-history or the PHS log directory if any.
      --output_folder=OUTPUT_FOLDER
          Type: str
          Default: '.'
          Base output directory. The final output will go into a subdirectory called wrapper-output.
          It will overwrite any existing directory with the same name.
        --gpu_cluster_props=GPU_CLUSTER_PROPS
            Type: Optional[str]
            Default: None
            Path of a file (json/yaml) containing configurations of the GPU cluster on which the Spark
            applications was run.
            The path is a local filesystem, or gstorage url.
            This option does not require the cluster to be live.
            When missing, the configurations are considered the same as the ones used by the execution cluster.
        --gpu_cluster_region=GPU_CLUSTER_REGION
            Type: Optional[str]
            Default: None
            The region where the GPU cluster belongs to. Note that this parameter requires 'gpu_cluster_props' to be
            defined.
            When missing, the region is set to the value passed in the 'region' argument.
        --gpu_cluster_zone=GPU_CLUSTER_ZONE
            Type: Optional[str]
            Default: None
            The zone where the GPU cluster belongs to. Note that this parameter requires 'gpu_cluster_props' to be
            defined.
            When missing, the zone is set to the same zone as the 'cluster' on which the Profiling tool is submitted.
      --debug=DEBUG
          Type: bool
          Default: False
          True or False to enable verbosity to the wrapper script.
      Additional flags are accepted.
        A list of valid Profiling tool options. Note that the wrapper ignores the following flags
        ["auto-tuner", "worker-info", "compare", "combined", "output-directory"]. For more details
        on Profiling tool options, please visit https://nvidia.github.io/spark-rapids/docs/spark-profiling-tool.html#profiling-tool-options.

  ```

- Example Running Profiling tool passing list of google storage directories
    - cmd
      ```
      spark_rapids_dataproc \
          profiling \
          --cluster=jobs-test-003 \
          --region=us-central1 \
          --eventlogs=gs://my-bucket/profile_testing/otherexamples/
      ```
    - result
      ```bash
      2022-09-23 13:25:17,040 INFO profiling: Preparing remote Work Env
      2022-09-23 13:25:18,242 INFO profiling: Upload Dependencies to Remote Cluster
      2022-09-23 13:25:20,163 INFO profiling: Running the tool as a spark job on dataproc
      2022-09-23 13:25:59,142 INFO profiling: Downloading the tool output
      2022-09-23 13:26:02,233 INFO profiling: Processing tool output
      Processing App app-20210507103057-0000
      Processing App app-20210413122423-0000
      Processing App app-20210507105707-0001
      Processing App app-20210422144630-0000
      Processing App app-20210609154416-0002
      ```

#### Running Profiling Tool with offline GPU cluster

Users can pass configuration files that describe the clusters involved in migrations. The files can
be stored locally or on GCS bucket.

- Format of the cluster configuration:
    - Both Json and Yaml formats are accepted
    - For the top level entry of configurations, both `config` and `cluster_config` are accepted.
    - `config` or `cluster_config` does not have to be the first entry in the yaml/json file. It can be
      nested.
    - For keys, the wrapper accepts camelCase and underscores keywords. i.e., `cluster_config` and `clusterConfig`
      are both accepted.
    - For the workers, the configuration has to indicate valid GPU accelerators.
    - Example of accepted file:
      ```yaml
        default:
          xyz_config:
            absdt_config:
              xyz_version: '2.0'
              team:
                tr_product_id: '1982'
            cluster_config:
              gce_cluster_config:
                metadata:
                  enable-pepperdata: 'true'
              master_config:
                disk_config:
                  boot_disk_size_gb: 100
                machine_type_uri: n1-standard-8
              secondary_worker_config:
                disk_config:
                  boot_disk_size_gb: 100
                machine_type_uri: n1-standard-32
                num_instances: 2
              software_config:
                image_version: '2.0'
              worker_config:
                disk_config:
                  boot_disk_size_gb: 100
                machine_type_uri: n1-standard-32
                num_instances: 2
                accelerators:
                - accelerator_count: 4
                  accelerator_type_uri: nvidia-tesla-t4
      ```

### Bootstrap Tool

Provides optimized RAPIDS Accelerator for Apache Spark configs based on Dataproc GPU cluster shape.
This tool is supposed to be used once a cluster has been created to set the recommended configurations.

#### Sample commands

- run the bootstrap tool help cmd `spark_rapids_dataproc bootstrap --help`
  ```bash
  NAME
      spark_rapids_dataproc bootstrap - The bootstrap tool analyzes the CPU and GPU configuration of
      the Dataproc cluster and updates the Spark default configuration on the cluster's master nodes.

  SYNOPSIS
      spark_rapids_dataproc bootstrap CLUSTER REGION <flags>

  DESCRIPTION
      The bootstrap tool analyzes the CPU and GPU configuration of the Dataproc cluster and updates the
      default configuration on the cluster's master nodes.

  POSITIONAL ARGUMENTS
      CLUSTER
          Type: str
          Name of the dataproc cluster
      REGION
          Type: str
          Compute region (e.g. us-central1) for the cluster.

  FLAGS
      --output_folder=OUTPUT_FOLDER
          Type: str
          Default: '.'
          Base output directory. The final recommendations will be logged in the subdirectory
          'wrapper-output/rapids_user_tools_bootstrap'. Note that this argument only accepts local
               filesystem.
      --dry_run=DRY_RUN
          Type: bool
          Default: False
          True or False to update the Spark config settings on Dataproc master node.
      --debug=DEBUG
          Type: bool
          Default: False
          True or False to enable verbosity to the wrapper script.
  ```

- Example Running bootstrap tool
    - cmd
      ```
      spark_rapids_dataproc \
          bootstrap \
          --cluster=jobs-test-003 \
          --region=us-central1
      ```
    - result
      ```bash
      Recommended configurations are saved to local disk: ./wrapper-output/rapids_user_tools_bootstrap/bootstrap_tool_output/rapids_4_dataproc_bootstrap_output.log
      Using the following computed settings based on worker nodes:
      ##### BEGIN : RAPIDS bootstrap settings for jobs-test-003
      spark.executor.cores=8
      spark.executor.memory=16384m
      spark.executor.memoryOverhead=5734m
      spark.rapids.sql.concurrentGpuTasks=2
      spark.rapids.memory.pinnedPool.size=4096m
      spark.sql.files.maxPartitionBytes=512m
      spark.task.resource.gpu.amount=0.125
      ##### END : RAPIDS bootstrap settings for jobs-test-003
      ```

### Diagnostic Tool

Validates the Dataproc with RAPIDS Accelerator for Apache Spark environment to make sure the cluster
is healthy and ready for Spark jobs.  
This tool can be used by the frontline support team for basic diagnostic and troubleshooting.

#### Sample commands

- Run the diagnostic tool help cmd `spark_rapids_dataproc diagnostic --help`

    ```text
    NAME
        spark_rapids_dataproc diagnostic - Run diagnostic on local environment or remote Dataproc cluster,
        such as check installed NVIDIA driver, CUDA toolkit, RAPIDS Accelerator for Apache Spark jar etc.
    
    SYNOPSIS
        spark_rapids_dataproc diagnostic CLUSTER REGION <flags>
    
    DESCRIPTION
        Run diagnostic on local environment or remote Dataproc cluster, such as check installed NVIDIA driver,
        CUDA toolkit, RAPIDS Accelerator for Apache Spark jar etc.
    
    POSITIONAL ARGUMENTS
        CLUSTER
            Type: str
            Name of the Dataproc cluster
        REGION
            Type: str
            Region of Dataproc cluster (e.g. us-central1)
    
    FLAGS
        --func=FUNC
            Type: str
            Default: 'all'
            Diagnostic function to run. Available functions: 'nv_driver': dump NVIDIA driver info via command
            `nvidia-smi`, 'cuda_version': check if CUDA toolkit major version >= 11.0, 'rapids_jar': check if
            only single RAPIDS Accelerator for Apache Spark jar is installed and verify its signature, 'deprecated_jar': check if deprecated
            (cudf) jar is installed. I.e. should no cudf jar starting with RAPIDS Accelerator for Apache Spark 22.08, 'spark': run a
            Hello-world Spark Application on CPU and GPU, 'perf': performance test for a Spark job between CPU and
            GPU, 'spark_job': run a Hello-world Spark Application on CPU and GPU via Dataproc job interface, 'perf_job':
            performance test for a Spark job between CPU and GPU via Dataproc job interface
        --debug=DEBUG Type: bool
            Default: False
            True or False to enable verbosity
    
    NOTES
        You can also use flags syntax for POSITIONAL ARGUMENTS
    ```

- Example running diagnostic tool

    - cmd

      ```bash
      spark_rapids_dataproc \
          diagnostic \
          --cluster=alex-demt \
          --region=us-central1 \
          nv_driver
      ```

    - result
      ```text
      *** Running diagnostic function "nv_driver" ***
      Warning: Permanently added 'compute.3346163243442954535' (ECDSA) to the list of known hosts.
      Wed Oct 19 02:32:36 2022
      +-----------------------------------------------------------------------------+
      | NVIDIA-SMI 460.106.00   Driver Version: 460.106.00   CUDA Version: 11.2     |
      |-------------------------------+----------------------+----------------------+
      | GPU  Name        Persistence-M| Bus-Id        Disp.A | Volatile Uncorr. ECC |
      | Fan  Temp  Perf  Pwr:Usage/Cap|         Memory-Usage | GPU-Util  Compute M. |
      |                               |                      |               MIG M. |
      |===============================+======================+======================|
      |   0  Tesla T4            On   | 00000000:00:04.0 Off |                    0 |
      | N/A   63C    P8    11W /  70W |      0MiB / 15109MiB |      0%      Default |
      |                               |                      |                  N/A |
      +-------------------------------+----------------------+----------------------+

      +-----------------------------------------------------------------------------+
      | Processes:                                                                  |
      |  GPU   GI   CI        PID   Type   Process name                  GPU Memory |
      |        ID   ID                                                   Usage      |
      |=============================================================================|
      |  No running processes found                                                 |
      +-----------------------------------------------------------------------------+
      Connection to 34.171.155.172 closed.
      *** Check "nv_driver": PASS ***
      *** Running diagnostic function "nv_driver" ***
      Warning: Permanently added 'compute.5880729710893392167' (ECDSA) to the list of known hosts.
      Wed Oct 19 02:32:42 2022
      +-----------------------------------------------------------------------------+
      | NVIDIA-SMI 460.106.00   Driver Version: 460.106.00   CUDA Version: 11.2     |
      |-------------------------------+----------------------+----------------------+
      | GPU  Name        Persistence-M| Bus-Id        Disp.A | Volatile Uncorr. ECC |
      | Fan  Temp  Perf  Pwr:Usage/Cap|         Memory-Usage | GPU-Util  Compute M. |
      |                               |                      |               MIG M. |
      |===============================+======================+======================|
      |   0  Tesla T4            On   | 00000000:00:04.0 Off |                    0 |
      | N/A   61C    P8    10W /  70W |      0MiB / 15109MiB |      0%      Default |
      |                               |                      |                  N/A |
      +-------------------------------+----------------------+----------------------+

      +-----------------------------------------------------------------------------+
      | Processes:                                                                  |
      |  GPU   GI   CI        PID   Type   Process name                  GPU Memory |
      |        ID   ID                                                   Usage      |
      |=============================================================================|
      |  No running processes found                                                 |
      +-----------------------------------------------------------------------------+
      Connection to 34.70.29.158 closed.
      *** Check "nv_driver": PASS ***
      ```
