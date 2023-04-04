# Validation tools on Dataproc

This is a guide for the Validation tools for Apache Spark on [Google Cloud Dataproc](https://cloud.google.com/dataproc).  
At the end of this guide, the user will be able to run the validation tools to analyze
whether the Spark job using RAPIDS Accelerator(aka GPU Spark job) returns the same result as the CPU Spark job.

## Prerequisites

### 1.gcloud CLI

- Install the gcloud CLI. Follow the instructions on [gcloud-sdk-install](https://cloud.google.com/sdk/docs/install)
- Set the configuration settings and credentials of the gcloud CLI:
  - Initialize the gcloud CLI by following [these instructions](https://cloud.google.com/sdk/docs/initializing#initialize_the)
  - Grant authorization to the gcloud CLI [with a user account](https://cloud.google.com/sdk/docs/authorizing#authorize_with_a_user_account)
  - Manage gcloud CLI configurations. For more details, visit [gcloud-sdk-configurations](https://cloud.google.com/sdk/docs/configurations)
  - Verify that the following [gcloud CLI properties](https://cloud.google.com/sdk/docs/properties) are properly defined:
    - `dataproc/region`,
    - `compute/zone`,
    - `compute/region`
    - `core/project`
    
### 2.Install the package

- Install `spark_rapids_validation_tool` with python [3.8, 3.10] using:
   ```bash
    git clone https://github.com/NVIDIA/spark-rapids-tools.git
    cd spark-rapids-tools/data_validation 
    python -m build --wheel
    pip uninstall spark_rapids_validation_tool-23.2.0-py3-none-any.whl -y
    pip install dist/spark_rapids_validation_tool-23.2.0-py3-none-any.whl
  ```
- verify the command is installed correctly by running
  ```bash
    spark_rapids_validation_tool validation --help
  ```

## Use case scenario
The validation tool has two type of command, one is for metadata validation, the other is for data validation.
The goal for metadata validation is to compare metadata of the columns of 2 tables quickly 
to see if there is any huge difference, the metadata info including table row count, column count, the min/max/avg
values for some specific columns.
The goal for dataset validation is to compare the table datasets column by column, and show any difference in 
the final result. The output includes the PK(s) only in table1, the PK(s) only in table2 and a
result table with the same PK(s) but different values for that column(s).

## Validation command
We support passing the parameter configuration to the command in the yml file format.
Below is an example about verifying datavliad3 and datavalid4 metadata information:
Template file:
```
sparkConf:
  spark.executor.memory: 4g
  spark.executor.cores: 3
  spark.executor.instances: 5
  spark.dynamicAllocation.enabled: false
toolConf:
  cluster: data-validation-test2
  region: us-central1
  check: valid_metadata
  format: hive
  table1: datavalid3
  table1_partition:
  table2: datavalid4
  table2_partition:
  pk:
  exclude_column:
  include_column: all
  filter:
  output_path:
  output_format:
  precision: 4
  debug: False
```
Tool command:
```
spark_rapids_validation_tool validation --conf_file=datavalid_conf.yml
```
