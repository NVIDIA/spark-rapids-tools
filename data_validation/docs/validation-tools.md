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
The goal for data validation is to compare the table datasets column by column, and show any difference in 
the final result.

## Validation command

```
spark_rapids_validation_tool validation --cluster=data-validation-test2 --region=us-central1 
--format=hive --t1=datavalid3 --t2=datavalid4  -p=2  -i=col2,col3,col4,col5 -e=col6,col7 valid_metadata
```

The output should be like:
```
+----------+------+------+------+------+-----------------+-----------------+-------------------+-------------------+-----------+-----------+
|ColumnName| min_A| min_B| max_A| max_B|            avg_A|            avg_B|           stddev_A|           stddev_B|countdist_A|countdist_B|
+----------+------+------+------+------+-----------------+-----------------+-------------------+-------------------+-----------+-----------+
|      col1|      |      |12.000|11.000|6.090909090909091|              6.0|  3.477198454346414|    3.3166247903554|           |           |
|      col3|      |      |      |      |5.090909090909091|5.181818181818182| 3.1766191290283903|  3.060005941764879|           |           |
|      col8|12.340|12.330|      |      |       12.4345455|       12.4336364|0.30031195901474267|0.30064173786328213|          3|          4|
+----------+------+------+------+------+-----------------+-----------------+-------------------+-------------------+-----------+-----------+
  
```
